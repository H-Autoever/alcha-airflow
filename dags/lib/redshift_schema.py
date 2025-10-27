"""
Redshift 테이블 스키마 정의 모듈.
"""

from __future__ import annotations

from textwrap import dedent

CREATE_TABLE_SQLS = [
    dedent(
        """
        CREATE TABLE IF NOT EXISTS realtime_data (
            vehicle_id VARCHAR(128),
            vehicle_speed DOUBLE PRECISION,
            engine_rpm BIGINT,
            engine_status_ignition VARCHAR(16),
            throttle_position DOUBLE PRECISION,
            wheel_speed_front_left DOUBLE PRECISION,
            wheel_speed_front_right DOUBLE PRECISION,
            wheel_speed_rear_left DOUBLE PRECISION,
            wheel_speed_rear_right DOUBLE PRECISION,
            gear_position_mode VARCHAR(16),
            gear_position_current_gear BIGINT,
            engine_temp DOUBLE PRECISION,
            coolant_temp DOUBLE PRECISION,
            side_brake_status VARCHAR(8),
            brake_pressure DOUBLE PRECISION,
            "timestamp" TIMESTAMP
        );
        """
    ),
    dedent(
        """
        CREATE TABLE IF NOT EXISTS periodic_data (
            vehicle_id VARCHAR(128),
            location_latitude DOUBLE PRECISION,
            location_longitude DOUBLE PRECISION,
            location_altitude DOUBLE PRECISION,
            temperature_cabin DOUBLE PRECISION,
            temperature_ambient DOUBLE PRECISION,
            alternator_output DOUBLE PRECISION,
            battery_voltage DOUBLE PRECISION,
            tpms_front_left DOUBLE PRECISION,
            tpms_front_right DOUBLE PRECISION,
            tpms_rear_left DOUBLE PRECISION,
            tpms_rear_right DOUBLE PRECISION,
            accelerometer_x DOUBLE PRECISION,
            accelerometer_y DOUBLE PRECISION,
            accelerometer_z DOUBLE PRECISION,
            fuel_level DOUBLE PRECISION,
            engine_coolant_temp DOUBLE PRECISION,
            transmission_oil_temp DOUBLE PRECISION,
            "timestamp" TIMESTAMP
        );
        """
    ),
    dedent(
        """
        CREATE TABLE IF NOT EXISTS collision (
            vehicle_id VARCHAR(128),
            damage DOUBLE PRECISION,
            "timestamp" TIMESTAMP
        );
        """
    ),
    dedent(
        """
        CREATE TABLE IF NOT EXISTS sudden_acceleration (
            vehicle_id VARCHAR(128),
            vehicle_speed DOUBLE PRECISION,
            throttle_position DOUBLE PRECISION,
            gear_position_mode VARCHAR(16),
            "timestamp" TIMESTAMP
        );
        """
    ),
    dedent(
        """
        CREATE TABLE IF NOT EXISTS engine_status (
            vehicle_id VARCHAR(128),
            vehicle_speed DOUBLE PRECISION,
            gear_position_mode VARCHAR(16),
            inclination_sensor DOUBLE PRECISION,
            side_brake_status VARCHAR(8),
            engine_status_ignition VARCHAR(16),
            "timestamp" TIMESTAMP,
            dct_count BIGINT,
            transmission_gear_change_count BIGINT,
            abs_activation_count BIGINT,
            suspension_shock_count BIGINT,
            adas_sensor_fault_count BIGINT,
            aeb_activation_count BIGINT,
            total_distance BIGINT,
            line_departure_warning_cont BIGINT
        );
        """
    ),
    dedent(
        """
        CREATE TABLE IF NOT EXISTS warning_light (
            vehicle_id VARCHAR(128),
            type VARCHAR(32),
            "timestamp" TIMESTAMP
        );
        """
    ),
    dedent(
        """
        CREATE TABLE IF NOT EXISTS driving_habit_monthly_stg (
            vehicle_id VARCHAR(128),
            analysis_month DATE,
            acceleration_events BIGINT,
            lane_departure_events BIGINT,
            night_drive_ratio DOUBLE PRECISION,
            avg_drive_duration_minutes DOUBLE PRECISION,
            avg_speed DOUBLE PRECISION,
            avg_distance DOUBLE PRECISION
        );
        """
    ),
]

__all__ = ["CREATE_TABLE_SQLS"]
