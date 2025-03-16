SELECT ticket_no, book_ref, passenger_id, passenger_name, contact_data::jsonb as contact_data
FROM "postgres"."public"."postgres_public_tickets"