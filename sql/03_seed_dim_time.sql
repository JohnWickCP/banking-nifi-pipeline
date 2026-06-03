-- Seed dim_time: 1440 rows (one per minute of the day)
-- Peak hours: 8-11h, 13-16h  |  Business hours: 8-17h

INSERT INTO dim_time (time_id, hour, minute, is_peak_hour, is_business_hour, period_label)
SELECT
    h * 100 + m                                                  AS time_id,
    h                                                            AS hour,
    m                                                            AS minute,
    h IN (8,9,10,11,13,14,15,16)                                 AS is_peak_hour,
    h >= 8 AND h < 17                                            AS is_business_hour,
    CASE
        WHEN h >= 6  AND h < 12 THEN 'morning'
        WHEN h >= 12 AND h < 18 THEN 'afternoon'
        WHEN h >= 18 AND h < 22 THEN 'evening'
        ELSE 'night'
    END                                                          AS period_label
FROM generate_series(0, 23) AS h
CROSS JOIN generate_series(0, 59) AS m
ON CONFLICT (time_id) DO NOTHING;
