{%- set source_model = "stg_airports" -%}
{%- set src_pk = "HUB_AIRPORTS_KEY" -%}
{%- set src_nk = "airport_code" -%}
{%- set src_ldts = "LOAD_DTS" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}