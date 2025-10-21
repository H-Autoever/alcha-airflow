from typing import List, Sequence

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook


def redshift_to_mysql(
    sql: str,
    target_table: str,
    target_fields: List[str],
    redshift_conn_id: str = "redshift_conn",
    mysql_conn_id: str = "mysql_analytics",
    commit_every: int = 1000,
) -> None:
    """Execute a SELECT on Redshift and insert the results into MySQL.

    Args:
        sql: SELECT query to run in Redshift.
        target_table: MySQL target table name (optionally with schema/db prefix).
        target_fields: Column names in MySQL to insert into, same order as SELECT.
        redshift_conn_id: Airflow connection ID for Redshift (Postgres-compatible).
        mysql_conn_id: Airflow connection ID for MySQL.
        commit_every: Batch size for inserts.
    """
    rshook = PostgresHook(postgres_conn_id=redshift_conn_id)
    rows: Sequence[tuple] = rshook.get_records(sql)
    if not rows:
        return

    myhook = MySqlHook(mysql_conn_id=mysql_conn_id)
    myhook.insert_rows(
        table=target_table,
        rows=rows,
        target_fields=target_fields,
        commit_every=commit_every,
        replace=False,
    )

