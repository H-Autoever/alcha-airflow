create table if not exists staging.realtime_data (
    vehicle_id varchar(64),
    vehicle_speed double precision,
    engine_rpm bigint,
    engine_status_ignition varchar(16),
    throttle_position double precision,
    wheel_speed_front_left double precision,
    wheel_speed_front_right double precision,
    wheel_speed_rear_left double precision,
    wheel_speed_rear_right double precision,
    gear_position_mode varchar(16),
    gear_position_current_gear bigint,
    gyro_yaw_rate double precision,
    gyro_pitch_rate double precision,
    gyro_roll_rate double precision,
    engine_temp double precision,
    coolant_temp double precision,
    ev_battery_voltage double precision,
    ev_battery_current double precision,
    ev_battery_soc double precision,
    "timestamp" timestamp
);

