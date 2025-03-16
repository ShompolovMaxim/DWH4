{%- set yaml_metadata -%}
source_model: 'flights'
derived_columns:
  RECORD_SOURCE: '!POSTGRES_MASTER'
hashed_columns:
  HUB_FLIGHTS_KEY: 'flight_id'
  HASHDIFF:
    is_hashdiff: true
    columns:
      - 'flight_no'
      - 'scheduled_departure'
      - 'scheduled_arrival'
      - 'status'
      - 'actual_departure'
      - 'actual_arrival'
  ARRIVAL_AIRPORT_KEY: 'arrival_airport'
  DEPARTURE_AIRPORT_KEY: 'departure_airport'
  HUB_AIRCRAFTS_KEY: 'aircraft_code'
  LINK_FLIGHTS_AIRCRAFTS_KEY:
    - 'flight_id'
    - 'aircraft_code'
  LINK_FLIGHTS_ARRIVAL_AIRPORT_KEY:
    - 'flight_id'
    - 'arrival_airport'
  LINK_FLIGHTS_DEPARTURE_AIRPORT_KEY:
    - 'flight_id'
    - 'departure_airport'

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}


WITH staging AS (
{{ automate_dv.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)

SELECT *, 
       ('{{ var('load_date') }}')::TIMESTAMP AS LOAD_DTS,
       ('{{ var('load_date') }}')::TIMESTAMP AS EFFECTIVE_FROM
FROM staging