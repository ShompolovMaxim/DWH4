{%- set source_model = "stg_ticket_flights" -%}
{%- set src_pk = "LINK_TICKET_FLIGHTS_KEY" -%}
{%- set src_fk = ["HUB_TICKETS_KEY", "HUB_FLIGHTS_KEY"] -%}
{%- set src_ldts = "LOAD_DTS" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                 src_source=src_source, source_model=source_model) }}
