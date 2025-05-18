INSERT INTO stg.boarding_passes (
    ticket_no,
    flight_id,
    boarding_no,
    seat_no,
    created_at,
    updated_at
) VALUES (
    %(ticket_no)s,
    %(flight_id)s,
    %(boarding_no)s,
    %(seat_no)s,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (ticket_no, flight_id) DO NOTHING;
