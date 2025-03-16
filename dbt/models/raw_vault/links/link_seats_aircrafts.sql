{%- set source_model = "stg_seats" -%}
{%- set src_pk = "LINK_SEATS_AIRCRAFTS_KEY" -%}
{%- set src_fk = ["HUB_AIRCRAFTS_KEY", "HUB_SEATS_KEY"] -%}
{%- set src_ldts = "LOAD_DTS" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                 src_source=src_source, source_model=source_model) }}
