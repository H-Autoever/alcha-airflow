from __future__ import annotations

from datetime import timedelta
import logging
from textwrap import dedent

import pandas as pd
import pendulum
import sqlalchemy
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from lib.redshift_schema import CREATE_TABLE_SQLS

logger = logging.getLogger(__name__)

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

MONTHLY_COPY_SOURCES = [
    ("copy_engine_status_prev_month", "engine_status", "event/engine_status"),
    ("copy_sudden_acc_prev_month", "sudden_acceleration", "event/sudden_acceleration"),
    ("copy_realtime_prev_month", "realtime_data", "realtime"),
]

ANALYSIS_MONTH_EXPR = "DATE_TRUNC('month', DATEADD(day, -1, '{{ ds }}'::DATE))"
ANALYSIS_MONTH_END_EXPR = (
    "DATEADD(month, 1, DATE_TRUNC('month', DATEADD(day, -1, '{{ ds }}'::DATE)))"
)


def build_monthly_copy_sql(table: str, s3_prefix: str) -> str:
    return dedent(
        f"""
        DELETE FROM {table}
        WHERE DATE_TRUNC('month', DATEADD(hour, 9, timestamp)) = {ANALYSIS_MONTH_EXPR}::DATE;

        COPY {table}
        FROM 's3://team-alcha-etl-bucket/curated/{s3_prefix}/year={{{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y") }}}}/month={{{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%m") }}}}/'
        IAM_ROLE default
        FORMAT AS PARQUET;
        """
    )


MONTHLY_AGGREGATE_SQL = dedent(
    f"""
    DELETE FROM driving_habit_monthly_stg
    WHERE analysis_month = {ANALYSIS_MONTH_EXPR}::DATE;

    INSERT INTO driving_habit_monthly_stg (
        vehicle_id,
        analysis_month,
        acceleration_events,
        lane_departure_events,
        night_drive_ratio,
        avg_drive_duration_minutes,
        avg_speed,
        avg_distance
    )
    SELECT
        es.vehicle_id,
        {ANALYSIS_MONTH_EXPR}::DATE AS analysis_month,
        COALESCE(sa.acceleration_events, 0) AS acceleration_events,
        GREATEST(
            0,
            COALESCE(MAX(es.line_departure_warning_cont) - MIN(es.line_departure_warning_cont), 0)
        ) AS lane_departure_events,
        ROUND(
            COALESCE(
                SUM(
                    CASE
                        WHEN DATE_PART(hour, DATEADD(hour, 9, es.timestamp)) BETWEEN 20 AND 23
                             OR DATE_PART(hour, DATEADD(hour, 9, es.timestamp)) BETWEEN 0 AND 5
                        THEN 1 ELSE 0 END
                )::FLOAT
                /
                NULLIF(COUNT(CASE WHEN es.engine_status_ignition = 'ON' THEN 1 END), 0),
                0
            ),
            3
        ) AS night_drive_ratio,
        ROUND(
            COALESCE(AVG(DATEDIFF(second, trip.start_time, trip.end_time)) / 60.0, 0),
            1
        ) AS avg_drive_duration_minutes,
        ROUND(COALESCE(AVG(rd.vehicle_speed), 0), 2) AS avg_speed,
        ROUND(
            COALESCE(
                (MAX(es.total_distance) - MIN(es.total_distance)) / NULLIF(COUNT(DISTINCT trip.trip_id), 0),
                0
            ),
            2
        ) AS avg_distance
    FROM engine_status es
    LEFT JOIN (
        SELECT
            vehicle_id,
            COUNT(*) AS acceleration_events
        FROM sudden_acceleration
        WHERE DATEADD(hour, 9, timestamp)::DATE >= {ANALYSIS_MONTH_EXPR}::DATE
          AND DATEADD(hour, 9, timestamp)::DATE < {ANALYSIS_MONTH_END_EXPR}::DATE
        GROUP BY vehicle_id
    ) sa ON sa.vehicle_id = es.vehicle_id
    LEFT JOIN (
        SELECT
            vehicle_id,
            ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY MIN(timestamp)) AS trip_id,
            MIN(timestamp) AS start_time,
            MAX(timestamp) AS end_time
        FROM engine_status
        WHERE engine_status_ignition = 'ON'
          AND DATEADD(hour, 9, timestamp)::DATE >= {ANALYSIS_MONTH_EXPR}::DATE
          AND DATEADD(hour, 9, timestamp)::DATE < {ANALYSIS_MONTH_END_EXPR}::DATE
        GROUP BY vehicle_id, DATE_TRUNC('day', DATEADD(hour, 9, timestamp))
    ) trip ON trip.vehicle_id = es.vehicle_id
    LEFT JOIN (
        SELECT
            vehicle_id,
            AVG(vehicle_speed) AS vehicle_speed
        FROM realtime_data
        WHERE vehicle_speed > 1
          AND engine_status_ignition = 'ON'
          AND DATEADD(hour, 9, timestamp)::DATE >= {ANALYSIS_MONTH_EXPR}::DATE
          AND DATEADD(hour, 9, timestamp)::DATE < {ANALYSIS_MONTH_END_EXPR}::DATE
        GROUP BY vehicle_id
    ) rd ON rd.vehicle_id = es.vehicle_id
    WHERE DATEADD(hour, 9, es.timestamp)::DATE >= {ANALYSIS_MONTH_EXPR}::DATE
      AND DATEADD(hour, 9, es.timestamp)::DATE < {ANALYSIS_MONTH_END_EXPR}::DATE
      AND es.engine_status_ignition = 'ON'
    GROUP BY
        es.vehicle_id,
        COALESCE(sa.acceleration_events, 0);
    """
)


def insert_monthly_habit_to_mysql(**context) -> None:
    execution_date = pendulum.instance(context["execution_date"])
    target_month = execution_date.subtract(months=1).start_of("month").date()

    redshift_hook = PostgresHook(postgres_conn_id="redshift")
    df = redshift_hook.get_pandas_df("SELECT * FROM driving_habit_monthly_stg;")

    if df.empty:
        logger.info("No monthly driving habit data to upsert. Skipping MySQL write.")
        return

    df["analysis_month"] = (
        pd.to_datetime(df["analysis_month"]).dt.to_period("M").dt.to_timestamp().dt.date
    )
    df = df[df["analysis_month"] == target_month]

    if df.empty:
        logger.info(
            "No monthly driving habit data for target month %s; skipping MySQL write.",
            target_month.strftime("%Y-%m"),
        )
        return

    mysql_conn = BaseHook.get_connection("mysql_local")
    engine = sqlalchemy.create_engine(
        f"mysql+mysqldb://{mysql_conn.login}:{mysql_conn.password}@{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
    )

    payload = df.to_dict(orient="records")

    upsert_sql = sqlalchemy.text(
        """
        INSERT INTO driving_habit_monthly (
            vehicle_id,
            analysis_month,
            acceleration_events,
            lane_departure_events,
            night_drive_ratio,
            avg_drive_duration_minutes,
            avg_speed,
            avg_distance
        ) VALUES (
            :vehicle_id,
            :analysis_month,
            :acceleration_events,
            :lane_departure_events,
            :night_drive_ratio,
            :avg_drive_duration_minutes,
            :avg_speed,
            :avg_distance
        )
        ON DUPLICATE KEY UPDATE
            acceleration_events = VALUES(acceleration_events),
            lane_departure_events = VALUES(lane_departure_events),
            night_drive_ratio = VALUES(night_drive_ratio),
            avg_drive_duration_minutes = VALUES(avg_drive_duration_minutes),
            avg_speed = VALUES(avg_speed),
            avg_distance = VALUES(avg_distance)
        """
    )

    with engine.begin() as conn:
        conn.execute(upsert_sql, payload)

    engine.dispose()


default_args = {
    "owner": "vehicle-analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="monthly_driving_habit_pipeline",
    description="Monthly driving habit statistics aggregation and insert into MySQL",
    default_args=default_args,
    schedule="30 0 1 * *",
    start_date=START_DATE,
    catchup=False,
    tags=["vehicle", "analytics", "monthly", "habit"],
) as dag:
    check_connection = PostgresOperator(
        task_id="check_redshift_connection",
        postgres_conn_id="redshift",
        sql="SELECT 1;",
        autocommit=True,
    )

    ensure_tables = PostgresOperator(
        task_id="ensure_redshift_tables",
        postgres_conn_id="redshift",
        sql=CREATE_TABLE_SQLS,
        autocommit=True,
    )

    copy_tasks = [
        PostgresOperator(
            task_id=task_id,
            postgres_conn_id="redshift",
            sql=build_monthly_copy_sql(table, prefix),
            autocommit=True,
        )
        for task_id, table, prefix in MONTHLY_COPY_SOURCES
    ]

    aggregate_task = PostgresOperator(
        task_id="aggregate_driving_habit_monthly",
        postgres_conn_id="redshift",
        sql=MONTHLY_AGGREGATE_SQL,
        autocommit=True,
    )

    insert_task = PythonOperator(
        task_id="insert_driving_habit_monthly_to_mysql",
        python_callable=insert_monthly_habit_to_mysql,
        provide_context=True,
    )

    check_connection >> ensure_tables
    for copy_task in copy_tasks:
        ensure_tables >> copy_task >> aggregate_task
    aggregate_task >> insert_task
