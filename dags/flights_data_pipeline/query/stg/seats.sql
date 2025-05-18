INSERT INTO stg.seats (
    aircraft_code,
    seat_no,
    fare_conditions,
    created_at,
    updated_at
) VALUES (
    %(aircraft_code)s,
    %(seat_no)s,
    %(fare_conditions)s,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (aircraft_code, seat_no) DO NOTHING;
