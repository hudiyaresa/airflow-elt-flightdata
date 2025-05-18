INSERT INTO stg.airports_data (
    airport_code,
    airport_name,
    city,
    coordinates,
    timezone,
    created_at,
    updated_at
) VALUES (
    %(airport_code)s,
    %(airport_name)s,
    %(city)s,
    %(coordinates)s,
    %(timezone)s,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (airport_code) DO NOTHING;
