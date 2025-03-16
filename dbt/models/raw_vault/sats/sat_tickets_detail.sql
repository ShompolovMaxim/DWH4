{%- set source_model = "stg_tickets" -%}
{%- set src_pk = "HUB_TICKETS_KEY" -%}
{%- set src_hashdiff = "HASHDIFF" -%}
{%- set src_payload = ["passenger_id", "passenger_name", "contact_data"] -%}
{%- set src_ldts = "LOAD_DTS" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
