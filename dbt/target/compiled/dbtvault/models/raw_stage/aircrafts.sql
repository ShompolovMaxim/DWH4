SELECT aircraft_code, model::jsonb as model, range
FROM "postgres"."public"."postgres_public_aircrafts"