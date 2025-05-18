INSERT INTO stg.ticket_flights (
    ticket_no,
    flight_id,
    fare_conditions,
    amount,
    created_at,
    updated_at
) VALUES (
    %(ticket_no)s,
    %(flight_id)s,
    %(fare_conditions)s,
    %(amount)s,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (ticket_no, flight_id) DO NOTHING;
