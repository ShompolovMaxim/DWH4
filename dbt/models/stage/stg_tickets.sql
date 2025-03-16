{%- set yaml_metadata -%}
source_model: 'tickets'
derived_columns:
  RECORD_SOURCE: '!POSTGRES_MASTER'
hashed_columns:
  HUB_TICKETS_KEY: 'ticket_no'
  HUB_BOOKINGS_KEY: 'book_ref'
  LINK_TICKETS_BOOKINGS_KEY:
    - 'ticket_no'
    - 'book_ref'
  HASHDIFF:
    is_hashdiff: true
    columns:
      - 'passenger_id'
      - 'passenger_name'
      - 'contact_data'

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