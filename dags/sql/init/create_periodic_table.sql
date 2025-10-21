create table if not exists staging.periodic_data (
    vehicle_id varchar(64),
    location_latitude double precision,
    location_longitude double precision,
    location_altitude double precision,
    temperature_cabin double precision,
    temperature_ambient double precision,
    battery_voltage double precision,
    tpms_front_left double precision,
    tpms_front_right double precision,
    tpms_rear_left double precision,
    tpms_rear_right double precision,
    accelerometer_x double precision,
    accelerometer_y double precision,
    accelerometer_z double precision,
    fuel_level double precision,
    "timestamp" timestamp
);

