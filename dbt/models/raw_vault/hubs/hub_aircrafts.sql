{%- set source_model = "stg_aircrafts" -%}
{%- set src_pk = "HUB_AIRCRAFTS_KEY" -%}
{%- set src_nk = "aircraft_code" -%}
{%- set src_ldts = "LOAD_DTS" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}