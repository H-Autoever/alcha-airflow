# dags/redshift_copy_load.py
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path

with DAG(
    dag_id="redshift_copy_load",
    start_date=datetime(2025, 10, 9),
    schedule_interval=None,     # 수동 실행
    catchup=False,
    tags=["redshift", "copy", "s3"],
) as dag:

    dag.template_searchpath = ['/opt/airflow/dags']

    copy_realtime_data = PostgresOperator(
        task_id="copy_realtime_data",
        postgres_conn_id="redshift_conn",
        sql="sql/copy/realtime_prev_day.sql",
    )

    copy_periodic_data = PostgresOperator(
        task_id="copy_periodic_data",
        postgres_conn_id="redshift_conn",
        sql="sql/copy/periodic_prev_day.sql",
    )

    def export_analysis_to_mysql_callable():
        sql_path = Path(__file__).resolve().parent / "sql" / "query" / "over_speed_risk.sql"
        sql_text = sql_path.read_text(encoding="utf-8")
        from lib.transfers import redshift_to_mysql
        redshift_to_mysql(
            sql=sql_text,
            target_table="alcha_db.insurance",
            target_fields=["vehicle_id", "over_speed_risk"],
            redshift_conn_id="redshift_conn",
            mysql_conn_id="mysql_analytics",
            commit_every=1000,
        )

    export_to_mysql = PythonOperator(
        task_id="export_to_mysql",
        python_callable=export_analysis_to_mysql_callable,
    )

    [copy_realtime_data, copy_periodic_data] >> export_to_mysql
