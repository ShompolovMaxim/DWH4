-- Generated by AutomateDV (formerly known as dbtvault)

    

WITH row_rank_1 AS (
    SELECT DISTINCT ON (rr.HUB_TICKETS_KEY) rr.HUB_TICKETS_KEY, rr.ticket_no, rr.LOAD_DTS, rr.RECORD_SOURCE
    FROM "postgres"."dwh_detailed"."stg_tickets" AS rr
    WHERE rr.HUB_TICKETS_KEY IS NOT NULL
    ORDER BY rr.HUB_TICKETS_KEY, rr.LOAD_DTS
),

records_to_insert AS (
    SELECT a.HUB_TICKETS_KEY, a.ticket_no, a.LOAD_DTS, a.RECORD_SOURCE
    FROM row_rank_1 AS a
    LEFT JOIN "postgres"."dwh_detailed"."hub_tickets" AS d
    ON a.HUB_TICKETS_KEY = d.HUB_TICKETS_KEY
    WHERE d.HUB_TICKETS_KEY IS NULL
)

SELECT * FROM records_to_insert