INSERT INTO stg.bookings (
    book_ref,
    book_date,
    total_amount,
    created_at,
    updated_at
) VALUES (
    %(book_ref)s,
    %(book_date)s,
    %(total_amount)s,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (book_ref) DO NOTHING;
