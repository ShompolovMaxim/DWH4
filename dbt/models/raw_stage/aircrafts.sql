SELECT aircraft_code, model::jsonb as model, range
FROM {{ source('postgres_master_data', 'aircrafts') }}