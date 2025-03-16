{%- set source_model = "stg_tickets" -%}
{%- set src_pk = "HUB_TICKETS_KEY" -%}
{%- set src_nk = "ticket_no" -%}
{%- set src_ldts = "LOAD_DTS" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}