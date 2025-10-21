from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="redshift_connect_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["redshift", "connection"],
    template_searchpath=["/opt/airflow/dags/sql"],
) as dag:

    test_connection = PostgresOperator(
        task_id="test_connection",
        postgres_conn_id="redshift_conn",
        sql="connect/test_connection.sql",
    )
