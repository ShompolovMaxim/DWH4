{%- set yaml_metadata -%}
source_model: 'boarding_passes'
derived_columns:
  RECORD_SOURCE: '!POSTGRES_MASTER'
hashed_columns:
  LINK_TICKET_FLIGHTS_KEY: 
      - 'ticket_no'
      - 'flight_id'
  HASHDIFF:
    is_hashdiff: true
    columns:
      - 'boarding_no'
      - 'seat_no'

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