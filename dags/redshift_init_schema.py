
# dags/redshift_init_schema.py
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="redshift_init_schema",
    start_date=datetime(2025, 10, 9),
    schedule_interval=None,
    catchup=False,
    template_searchpath=["/opt/airflow/dags/sql"],
) as dag:

    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="redshift",
        sql="init/create_schema.sql",
    )

    create_realtime_table = PostgresOperator(
        task_id="create_realtime_table",
        postgres_conn_id="redshift",
        sql="init/create_realtime_table.sql",
    )

    create_periodic_table = PostgresOperator(
        task_id="create_periodic_table",
        postgres_conn_id="redshift",
        sql="init/create_periodic_table.sql",
    )

    create_schema >> [create_realtime_table, create_periodic_table]
