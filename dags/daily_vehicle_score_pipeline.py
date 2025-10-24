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

COPY_SOURCES = [
    ("copy_realtime_prev_day", "realtime_data", "realtime"),
    ("copy_periodic_prev_day", "periodic_data", "periodic"),
    ("copy_engine_status_prev_day", "engine_status", "event/engine_status"),
    ("copy_sudden_acc_prev_day", "sudden_acceleration", "event/sudden_acceleration"),
]


def build_daily_copy_sql(table: str, s3_prefix: str) -> str:
    return dedent(
        f"""
        DELETE FROM {table}
        WHERE DATEADD(hour, 9, timestamp)::DATE = '{{{{ macros.ds_add(ds, -1) }}}}'::DATE;

        COPY {table}
        FROM 's3://team-alcha-etl-stage/curated/{s3_prefix}/year={{{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y") }}}}/month={{{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%m") }}}}/day={{{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%d") }}}}/'
        IAM_ROLE default
        FORMAT AS PARQUET;
        """
    )


DAILY_AGGREGATE_SQL = dedent(
    """
    DROP TABLE IF EXISTS vehicle_daily_aggregated_stg;

    CREATE TABLE vehicle_daily_aggregated_stg AS
    WITH realtime_avg AS (
        SELECT
            vehicle_id,
            AVG(engine_rpm) AS engine_rpm_avg,
            AVG(brake_pressure) AS brake_pressure
        FROM realtime_data
        WHERE DATEADD(hour, 9, timestamp)::DATE = '{{ macros.ds_add(ds, -1) }}'::DATE
          AND gear_position_mode = 'D'
          AND vehicle_speed > 1
          AND engine_status_ignition = 'ON'
        GROUP BY vehicle_id
    ),
    periodic_avg AS (
        SELECT
            vehicle_id,
            AVG(engine_coolant_temp) AS engine_coolant_temp_avg,
            AVG(transmission_oil_temp) AS transmission_oil_temp_avg,
            AVG(battery_voltage) AS battery_voltage_avg,
            AVG(alternator_output) AS alternator_output_avg,
            AVG(temperature_ambient) AS temperature_ambient_avg
        FROM periodic_data
        WHERE DATEADD(hour, 9, timestamp)::DATE = '{{ macros.ds_add(ds, -1) }}'::DATE
        GROUP BY vehicle_id
    ),
    engine_counts AS (
        SELECT
            vehicle_id,
            MAX(dct_count) - MIN(dct_count) AS dtc_count,
            MAX(transmission_gear_change_count) - MIN(transmission_gear_change_count) AS gear_change_count,
            MAX(abs_activation_count) - MIN(abs_activation_count) AS abs_activation_count,
            MAX(suspension_shock_count) - MIN(suspension_shock_count) AS suspension_shock_count,
            MAX(adas_sensor_fault_count) - MIN(adas_sensor_fault_count) AS adas_sensor_fault_count,
            MAX(aeb_activation_count) - MIN(aeb_activation_count) AS aeb_activation_count,
            COUNT(CASE WHEN engine_status_ignition = 'ON' THEN 1 END) AS engine_start_count
        FROM engine_status
        WHERE DATEADD(hour, 9, timestamp)::DATE = '{{ macros.ds_add(ds, -1) }}'::DATE
        GROUP BY vehicle_id
    ),
    suddenacc_counts AS (
        SELECT
            vehicle_id,
            COUNT(*) AS suddenacc_count
        FROM sudden_acceleration
        WHERE DATEADD(hour, 9, timestamp)::DATE = '{{ macros.ds_add(ds, -1) }}'::DATE
        GROUP BY vehicle_id
    )
    SELECT
        e.vehicle_id,
        '{{ macros.ds_add(ds, -1) }}'::DATE AS analysis_date,
        COALESCE(r.engine_rpm_avg, 0) AS engine_rpm_avg,
        COALESCE(p.engine_coolant_temp_avg, 0) AS engine_coolant_temp_avg,
        COALESCE(p.transmission_oil_temp_avg, 0) AS transmission_oil_temp_avg,
        COALESCE(p.battery_voltage_avg, 13.5) AS battery_voltage_avg,
        COALESCE(p.alternator_output_avg, 0) AS alternator_output_avg,
        COALESCE(p.temperature_ambient_avg, 20) AS temperature_ambient_avg,
        COALESCE(e.dtc_count, 0) AS dtc_count,
        COALESCE(e.gear_change_count, 0) AS gear_change_count,
        COALESCE(e.abs_activation_count, 0) AS abs_activation_count,
        COALESCE(e.suspension_shock_count, 0) AS suspension_shock_count,
        COALESCE(e.adas_sensor_fault_count, 0) AS adas_sensor_fault_count,
        COALESCE(e.aeb_activation_count, 0) AS aeb_activation_count,
        COALESCE(e.engine_start_count, 0) AS engine_start_count,
        COALESCE(s.suddenacc_count, 0) AS suddenacc_count,
        COALESCE(r.brake_pressure, 30) AS brake_pressure
    FROM engine_counts e
    LEFT JOIN realtime_avg r ON e.vehicle_id = r.vehicle_id
    LEFT JOIN periodic_avg p ON e.vehicle_id = p.vehicle_id
    LEFT JOIN suddenacc_counts s ON e.vehicle_id = s.vehicle_id;
    """
)


def calculate_and_insert_scores(**_: dict) -> None:
    redshift_hook = PostgresHook(postgres_conn_id="redshift")
    df = redshift_hook.get_pandas_df("SELECT * FROM vehicle_daily_aggregated_stg;")

    if df.empty:
        logger.info("No vehicle data found for scoring. Skipping MySQL upsert.")
        return

    df["analysis_date"] = pd.to_datetime(df["analysis_date"]).dt.date
    df = df.fillna(
        {
            "engine_rpm_avg": 0.0,
            "engine_coolant_temp_avg": 85.0,
            "transmission_oil_temp_avg": 85.0,
            "battery_voltage_avg": 13.5,
            "alternator_output_avg": 0.0,
            "temperature_ambient_avg": 20.0,
            "dtc_count": 0,
            "gear_change_count": 0,
            "abs_activation_count": 0,
            "suspension_shock_count": 0,
            "adas_sensor_fault_count": 0,
            "aeb_activation_count": 0,
            "engine_start_count": 0,
            "suddenacc_count": 0,
            "brake_pressure": 30.0,
        }
    )

    def clip(value: float, minimum: float, maximum: float) -> float:
        return max(minimum, min(value, maximum))

    def calc_engine(row: pd.Series) -> float:
        rpm = clip(100 - max(0, row.engine_rpm_avg - 3000) * 0.05, 0, 100)
        coolant = clip(100 - abs(85 - row.engine_coolant_temp_avg) * 2, 0, 100)
        dtc = clip(100 - row.dtc_count * 25, 0, 100)
        return round(rpm * 0.3 + coolant * 0.5 + dtc * 0.2, 2)

    def calc_trans(row: pd.Series) -> float:
        oil = clip(100 - abs(85 - row.transmission_oil_temp_avg) * 2, 0, 100)
        gear = clip(100 - max(0, row.gear_change_count - 150) * 0.2, 0, 100)
        return round(oil * 0.6 + gear * 0.4, 2)

    def calc_brake(row: pd.Series) -> float:
        abs_ = clip(100 - max(0, row.abs_activation_count - 5) * 5, 0, 100)
        susp = clip(100 - max(0, row.suspension_shock_count - 50) * 0.4, 0, 100)
        brakep = clip(100 - abs(30 - row.brake_pressure) * 2, 0, 100)
        return round((abs_ + susp + brakep) / 3, 2)

    def calc_adas(row: pd.Series) -> float:
        adas = clip(100 - max(0, row.adas_sensor_fault_count - 1) * 10, 0, 100)
        aeb = clip(100 - max(0, row.aeb_activation_count - 1) * 10, 0, 100)
        return round((adas + aeb) / 2, 2)

    def calc_elec(row: pd.Series) -> float:
        volt = clip(100 - abs(13.5 - row.battery_voltage_avg) * 10, 0, 100)
        start = clip(100 - max(0, row.engine_start_count - 5) * 10, 0, 100)
        return round(volt * 0.8 + start * 0.2, 2)

    def calc_other(row: pd.Series) -> float:
        acc = clip(100 - max(0, row.suddenacc_count - 3) * 5, 0, 100)
        temp = clip(100 - abs(20 - row.temperature_ambient_avg) * 1, 0, 100)
        return round(acc * 0.7 + temp * 0.3, 2)

    df["engine_powertrain_score"] = df.apply(calc_engine, axis=1)
    df["transmission_drivetrain_score"] = df.apply(calc_trans, axis=1)
    df["brake_suspension_score"] = df.apply(calc_brake, axis=1)
    df["adas_safety_score"] = df.apply(calc_adas, axis=1)
    df["electrical_battery_score"] = df.apply(calc_elec, axis=1)
    df["other_score"] = df.apply(calc_other, axis=1)

    df["final_score"] = round(
        df.engine_powertrain_score * 0.25
        + df.transmission_drivetrain_score * 0.15
        + df.brake_suspension_score * 0.15
        + df.adas_safety_score * 0.15
        + df.electrical_battery_score * 0.10
        + df.other_score * 0.20,
        2,
    )

    mysql_conn = BaseHook.get_connection("mysql_local")
    engine = sqlalchemy.create_engine(
        f"mysql+mysqldb://{mysql_conn.login}:{mysql_conn.password}@{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
    )

    metric_columns = [
        "engine_rpm_avg",
        "engine_coolant_temp_avg",
        "transmission_oil_temp_avg",
        "battery_voltage_avg",
        "alternator_output_avg",
        "temperature_ambient_avg",
        "dtc_count",
        "gear_change_count",
        "abs_activation_count",
        "suspension_shock_count",
        "adas_sensor_fault_count",
        "aeb_activation_count",
        "engine_start_count",
        "suddenacc_count",
    ]

    payload = df[
        [
            "vehicle_id",
            "analysis_date",
            *metric_columns,
            "final_score",
            "engine_powertrain_score",
            "transmission_drivetrain_score",
            "brake_suspension_score",
            "adas_safety_score",
            "electrical_battery_score",
            "other_score",
        ]
    ].to_dict(orient="records")

    if not payload:
        logger.info("No rows to upsert into MySQL vehicle_score_daily.")
        engine.dispose()
        return

    upsert_sql = sqlalchemy.text(
        """
        INSERT INTO vehicle_score_daily (
            vehicle_id,
            analysis_date,
            engine_rpm_avg,
            engine_coolant_temp_avg,
            transmission_oil_temp_avg,
            battery_voltage_avg,
            temperature_ambient_avg,
            dtc_count,
            gear_change_count,
            abs_activation_count,
            suspension_shock_count,
            adas_sensor_fault_count,
            aeb_activation_count,
            engine_start_count,
            suddenacc_count,
            final_score,
            engine_powertrain_score,
            transmission_drivetrain_score,
            brake_suspension_score,
            adas_safety_score,
            electrical_battery_score,
            other_score
        ) VALUES (
            :vehicle_id,
            :analysis_date,
            :engine_rpm_avg,
            :engine_coolant_temp_avg,
            :transmission_oil_temp_avg,
            :battery_voltage_avg,
            :temperature_ambient_avg,
            :dtc_count,
            :gear_change_count,
            :abs_activation_count,
            :suspension_shock_count,
            :adas_sensor_fault_count,
            :aeb_activation_count,
            :engine_start_count,
            :suddenacc_count,
            :final_score,
            :engine_powertrain_score,
            :transmission_drivetrain_score,
            :brake_suspension_score,
            :adas_safety_score,
            :electrical_battery_score,
            :other_score
        )
        ON DUPLICATE KEY UPDATE
            final_score = VALUES(final_score),
            engine_powertrain_score = VALUES(engine_powertrain_score),
            transmission_drivetrain_score = VALUES(transmission_drivetrain_score),
            brake_suspension_score = VALUES(brake_suspension_score),
            adas_safety_score = VALUES(adas_safety_score),
            electrical_battery_score = VALUES(electrical_battery_score),
            other_score = VALUES(other_score),
            engine_rpm_avg = VALUES(engine_rpm_avg),
            engine_coolant_temp_avg = VALUES(engine_coolant_temp_avg),
            transmission_oil_temp_avg = VALUES(transmission_oil_temp_avg),
            battery_voltage_avg = VALUES(battery_voltage_avg),
            temperature_ambient_avg = VALUES(temperature_ambient_avg),
            dtc_count = VALUES(dtc_count),
            gear_change_count = VALUES(gear_change_count),
            abs_activation_count = VALUES(abs_activation_count),
            suspension_shock_count = VALUES(suspension_shock_count),
            adas_sensor_fault_count = VALUES(adas_sensor_fault_count),
            aeb_activation_count = VALUES(aeb_activation_count),
            engine_start_count = VALUES(engine_start_count),
            suddenacc_count = VALUES(suddenacc_count)
        """
    )

    with engine.begin() as conn:
        conn.execute(upsert_sql, payload)

    engine.dispose()


default_args = {
    "owner": "vehicle-analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_vehicle_score_pipeline",
    description="Daily aggregation and score calculation for vehicle data",
    default_args=default_args,
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
    tags=["vehicle", "analytics", "score"],
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
            sql=build_daily_copy_sql(table, prefix),
            autocommit=True,
        )
        for task_id, table, prefix in COPY_SOURCES
    ]

    aggregate_task = PostgresOperator(
        task_id="aggregate_daily_vehicle_data",
        postgres_conn_id="redshift",
        sql=DAILY_AGGREGATE_SQL,
        autocommit=True,
    )

    score_task = PythonOperator(
        task_id="calculate_and_insert_vehicle_scores",
        python_callable=calculate_and_insert_scores,
    )

    check_connection >> ensure_tables
    for copy_task in copy_tasks:
        ensure_tables >> copy_task >> aggregate_task
    aggregate_task >> score_task

