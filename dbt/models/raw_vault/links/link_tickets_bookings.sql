{%- set source_model = "stg_tickets" -%}
{%- set src_pk = "LINK_TICKETS_BOOKINGS_KEY" -%}
{%- set src_fk = ["HUB_TICKETS_KEY", "HUB_BOOKINGS_KEY"] -%}
{%- set src_ldts = "LOAD_DTS" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                 src_source=src_source, source_model=source_model) }}
