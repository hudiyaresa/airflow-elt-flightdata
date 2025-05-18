INSERT INTO stg.flights (
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    created_at,
    updated_at
) VALUES (
    %(flight_id)s,
    %(flight_no)s,
    %(scheduled_departure)s,
    %(scheduled_arrival)s,
    %(departure_airport)s,
    %(arrival_airport)s,
    %(status)s,
    %(aircraft_code)s,
    %(actual_departure)s,
    %(actual_arrival)s,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (flight_id) DO NOTHING;
