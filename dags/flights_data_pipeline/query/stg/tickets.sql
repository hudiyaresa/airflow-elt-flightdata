INSERT INTO stg.tickets (
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    contact_data,
    created_at,
    updated_at
) VALUES (
    %(ticket_no)s,
    %(book_ref)s,
    %(passenger_id)s,
    %(passenger_name)s,
    %(contact_data)s,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (ticket_no) DO NOTHING;
