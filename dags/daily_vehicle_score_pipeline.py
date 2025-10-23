from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pandas as pd
import sqlalchemy

# -----------------------------------
#  DAG 기본 설정
# -----------------------------------
default_args = {
    "owner": "vehicle-analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="daily_vehicle_score_pipeline",
    description="Daily aggregation and score calculation for vehicle data",
    default_args=default_args,
    # schedule_interval="0 0 * * *",  # 매일 자정
    schedule_interval=None,  # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["vehicle", "analytics", "score"],
)

# -----------------------------------
#  Step 1️⃣ Redshift - 하루치 데이터 집계
# -----------------------------------
aggregate_sql = """
CREATE TEMP TABLE vehicle_daily_aggregated AS
WITH realtime_avg AS (
    SELECT
        vehicle_id,
        AVG(engine_rpm) AS engine_rpm_avg,
        AVG(brake_pressure) AS brake_pressure
    FROM realtime_data
    WHERE DATEADD(hour, 9, timestamp)::DATE = DATEADD(day, -1, GETDATE())::DATE
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
        AVG(temperature_ambient) AS temperature_ambient_avg
    FROM periodic_data
    WHERE DATEADD(hour, 9, timestamp)::DATE = DATEADD(day, -1, GETDATE())::DATE
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
    WHERE DATEADD(hour, 9, timestamp)::DATE = DATEADD(day, -1, GETDATE())::DATE
    GROUP BY vehicle_id
),
suddenacc_counts AS (
    SELECT vehicle_id, COUNT(*) AS suddenacc_count
    FROM sudden_acceleration
    WHERE DATEADD(hour, 9, timestamp)::DATE = DATEADD(day, -1, GETDATE())::DATE
    GROUP BY vehicle_id
)
SELECT
    e.vehicle_id,
    r.engine_rpm_avg,
    p.engine_coolant_temp_avg,
    p.transmission_oil_temp_avg,
    p.battery_voltage_avg,
    p.temperature_ambient_avg,
    e.dtc_count,
    e.gear_change_count,
    e.abs_activation_count,
    e.suspension_shock_count,
    e.adas_sensor_fault_count,
    e.aeb_activation_count,
    e.engine_start_count,
    s.suddenacc_count,
    r.brake_pressure
FROM engine_counts e
LEFT JOIN realtime_avg r ON e.vehicle_id = r.vehicle_id
LEFT JOIN periodic_avg p ON e.vehicle_id = p.vehicle_id
LEFT JOIN suddenacc_counts s ON e.vehicle_id = s.vehicle_id;
"""

aggregate_task = PostgresOperator(
    task_id="aggregate_daily_vehicle_data",
    postgres_conn_id="redshift",  # ✅ Redshift 연결 ID
    sql=aggregate_sql,
    dag=dag,
)

# -----------------------------------
#  Step 2️⃣ PythonOperator - 점수 계산 및 MySQL 업서트
# -----------------------------------

def calculate_and_insert_scores(**context):
    import psycopg2
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # Redshift 연결 (PostgresHook 사용)
    redshift_hook = PostgresHook(postgres_conn_id="redshift")
    redshift_conn = redshift_hook.get_conn()
    redshift_cursor = redshift_conn.cursor()

    redshift_cursor.execute("SELECT * FROM vehicle_daily_aggregated;")
    columns = [desc[0] for desc in redshift_cursor.description]
    data = redshift_cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    # ---- 점수 계산 ----
    def clip(v, minv, maxv): return max(minv, min(v, maxv))

    def calc_engine(row):
        rpm = clip(100 - max(0, row.engine_rpm_avg - 3000) * 0.05, 0, 100)
        coolant = clip(100 - abs(85 - row.engine_coolant_temp_avg) * 2, 0, 100)
        dtc = clip(100 - row.dtc_count * 25, 0, 100)
        return round(rpm * 0.3 + coolant * 0.5 + dtc * 0.2, 2)

    def calc_trans(row):
        oil = clip(100 - abs(85 - row.transmission_oil_temp_avg) * 2, 0, 100)
        gear = clip(100 - max(0, row.gear_change_count - 150) * 0.2, 0, 100)
        return round(oil * 0.6 + gear * 0.4, 2)

    def calc_brake(row):
        abs_ = clip(100 - max(0, row.abs_activation_count - 5) * 5, 0, 100)
        susp = clip(100 - max(0, row.suspension_shock_count - 50) * 0.4, 0, 100)
        brakep = clip(100 - abs(30 - row.brake_pressure) * 2, 0, 100)
        return round((abs_ + susp + brakep) / 3, 2)

    def calc_adas(row):
        adas = clip(100 - max(0, row.adas_sensor_fault_count - 1) * 10, 0, 100)
        aeb = clip(100 - max(0, row.aeb_activation_count - 1) * 10, 0, 100)
        return round((adas + aeb) / 2, 2)

    def calc_elec(row):
        volt = clip(100 - abs(13.5 - row.battery_voltage_avg) * 10, 0, 100)
        start = clip(100 - max(0, row.engine_start_count - 5) * 10, 0, 100)
        return round(volt * 0.8 + start * 0.2, 2)

    def calc_other(row):
        acc = clip(100 - max(0, row.suddenacc_count - 3) * 5, 0, 100)
        temp = clip(100 - abs(20 - row.temperature_ambient_avg) * 1, 0, 100)
        return round(acc * 0.7 + temp * 0.3, 2)

    # 계산
    df["engine_powertrain_score"] = df.apply(calc_engine, axis=1)
    df["transmission_drivetrain_score"] = df.apply(calc_trans, axis=1)
    df["brake_suspension_score"] = df.apply(calc_brake, axis=1)
    df["adas_safety_score"] = df.apply(calc_adas, axis=1)
    df["electrical_battery_score"] = df.apply(calc_elec, axis=1)
    df["other_score"] = df.apply(calc_other, axis=1)

    # 종합 점수
    df["final_score"] = round(
        df.engine_powertrain_score * 0.25 +
        df.transmission_drivetrain_score * 0.15 +
        df.brake_suspension_score * 0.15 +
        df.adas_safety_score * 0.15 +
        df.electrical_battery_score * 0.10 +
        df.other_score * 0.20, 2
    )

    # MySQL 연결
    mysql_conn = BaseHook.get_connection("mysql_local")
    engine = sqlalchemy.create_engine(
        f"mysql+pymysql://{mysql_conn.login}:{mysql_conn.password}@{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
    )

    df["analysis_date"] = (datetime.utcnow() + timedelta(hours=9) - timedelta(days=1)).date()
    df[
        [
            "vehicle_id", "analysis_date", "final_score",
            "engine_powertrain_score", "transmission_drivetrain_score",
            "brake_suspension_score", "adas_safety_score",
            "electrical_battery_score", "other_score"
        ]
    ].to_sql(
        "vehicle_score_daily",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    engine.dispose()
    redshift_cursor.close()
    redshift_conn.close()

score_task = PythonOperator(
    task_id="calculate_and_insert_vehicle_scores",
    python_callable=calculate_and_insert_scores,
    provide_context=True,
    dag=dag,
)

aggregate_task >> score_task