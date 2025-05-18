INSERT INTO stg.aircrafts_data (
    aircraft_code,
    model,
    range,
    created_at,
    updated_at
) VALUES (
    %(aircraft_code)s,
    %(model)s,
    %(range)s,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (aircraft_code) DO NOTHING;
