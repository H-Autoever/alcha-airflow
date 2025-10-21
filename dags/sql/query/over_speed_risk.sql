SELECT  
    vehicle_id,
    CASE 
        WHEN MAX(vehicle_speed) > 120 AND COUNT(CASE WHEN vehicle_speed > 100 THEN 1 END) > 20 THEN 95
        WHEN MAX(vehicle_speed) > 120 AND COUNT(CASE WHEN vehicle_speed > 100 THEN 1 END) > 10 THEN 85
        WHEN MAX(vehicle_speed) > 100 AND COUNT(CASE WHEN vehicle_speed > 80 THEN 1 END) > 30 THEN 75
        WHEN MAX(vehicle_speed) > 100 AND COUNT(CASE WHEN vehicle_speed > 80 THEN 1 END) > 15 THEN 65
        WHEN MAX(vehicle_speed) > 80 AND COUNT(CASE WHEN vehicle_speed > 80 THEN 1 END) > 20 THEN 55
        WHEN MAX(vehicle_speed) > 80 AND COUNT(CASE WHEN vehicle_speed > 80 THEN 1 END) > 10 THEN 45
        WHEN MAX(vehicle_speed) > 80 AND COUNT(CASE WHEN vehicle_speed > 80 THEN 1 END) > 5 THEN 35
        WHEN MAX(vehicle_speed) > 80 AND COUNT(CASE WHEN vehicle_speed > 80 THEN 1 END) > 0 THEN 25
        ELSE 15
    END AS over_speed_risk
FROM staging.realtime_data
GROUP BY vehicle_id;

