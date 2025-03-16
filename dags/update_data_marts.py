import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


# ---------- ПЕРЕМЕННЫЕ/КОНСТАНТЫ ----------

sql_script_1 = '''
    CREATE SCHEMA IF NOT EXISTS presentation;

    DROP TABLE IF EXISTS presentation.task1;

    CREATE TABLE presentation.task1 AS

    WITH passenger_airport_cnt AS (
    SELECT sq.passenger_id as passenger_id, sq.airport_code as airport_code, COUNT(sq.hub_flights_key) as cnt
    FROM ((
        SELECT st.passenger_id as passenger_id, had.airport_code as airport_code, hf.hub_flights_key as hub_flights_key
        FROM dwh_detailed.hub_tickets as ht
        JOIN dwh_detailed.sat_tickets_detail st ON (ht.hub_tickets_key = st.hub_tickets_key)
        JOIN dwh_detailed.link_ticket_flights ltf ON (ht.hub_tickets_key = ltf.hub_tickets_key)
        JOIN dwh_detailed.hub_flights hf ON (hf.hub_flights_key = ltf.hub_flights_key)
        JOIN dwh_detailed.link_flights_departure_airport lfda ON (hf.hub_flights_key = lfda.hub_flights_key)
        JOIN dwh_detailed.hub_airports had ON (had.hub_airports_key = lfda.departure_airport_key)
        WHERE st.load_dts = (SELECT MAX(st2.load_dts) FROM dwh_detailed.sat_tickets_detail st2 WHERE st2.hub_tickets_key = st.hub_tickets_key)
    ) UNION (
        SELECT st.passenger_id as passenger_id, haa.airport_code as airport_code, hf.hub_flights_key as hub_flights_key
        FROM dwh_detailed.hub_tickets as ht
        JOIN dwh_detailed.sat_tickets_detail st ON (ht.hub_tickets_key = st.hub_tickets_key)
        JOIN dwh_detailed.link_ticket_flights ltf ON (ht.hub_tickets_key = ltf.hub_tickets_key)
        JOIN dwh_detailed.hub_flights hf ON (hf.hub_flights_key = ltf.hub_flights_key)
        JOIN dwh_detailed.link_flights_arrival_airport lfaa ON (hf.hub_flights_key = lfaa.hub_flights_key)
        JOIN dwh_detailed.hub_airports haa ON (haa.hub_airports_key = lfaa.arrival_airport_key)
        WHERE st.load_dts = (SELECT MAX(st2.load_dts) FROM dwh_detailed.sat_tickets_detail st2 WHERE st2.hub_tickets_key = st.hub_tickets_key)
    )) as sq
    GROUP BY sq.passenger_id, sq.airport_code),

    passenger_airport_cnt_max AS (
    SELECT pac.passenger_id as passenger_id, MAX(pac.cnt) as max_cnt
    FROM passenger_airport_cnt pac
    GROUP BY pac.passenger_id),

    passenger_airport_max AS (
    SELECT
        pac1.passenger_id as passenger_id,
        MIN(pac1.airport_code) as airport_code
    FROM passenger_airport_cnt pac1
    WHERE pac1.cnt = (SELECT max_cnt FROM passenger_airport_cnt_max pac2 where pac2.passenger_id = pac1.passenger_id)
    GROUP BY pac1.passenger_id),

    passenger_data AS (
    SELECT
        st.passenger_id as passenger_id,
        MIN(st.passenger_name) as passenger_name,
        COUNT(ltf.link_ticket_flights_key) as flights_number,
        SUM(stfd.amount) as purchase_sum
    FROM dwh_detailed.hub_tickets as ht
    JOIN dwh_detailed.sat_tickets_detail st ON (ht.hub_tickets_key = st.hub_tickets_key)
    JOIN dwh_detailed.link_ticket_flights ltf ON (ht.hub_tickets_key = ltf.hub_tickets_key)
    JOIN dwh_detailed.sat_ticket_flights_detail stfd ON (ltf.link_ticket_flights_key = stfd.link_ticket_flights_key)
    WHERE st.load_dts = (SELECT MAX(st2.load_dts) FROM dwh_detailed.sat_tickets_detail st2 WHERE st2.hub_tickets_key = st.hub_tickets_key) AND
          stfd.load_dts = (SELECT MAX(stfd2.load_dts) FROM dwh_detailed.sat_ticket_flights_detail stfd2 WHERE stfd2.link_ticket_flights_key = stfd.link_ticket_flights_key)
    GROUP BY st.passenger_id),

    passenger_gmv AS(
    SELECT st.passenger_id as passenger_id, SUM(stfd.amount) as gmv
    FROM dwh_detailed.hub_tickets as ht
    JOIN dwh_detailed.sat_tickets_detail st ON (ht.hub_tickets_key = st.hub_tickets_key)
    JOIN dwh_detailed.link_ticket_flights ltf ON (ht.hub_tickets_key = ltf.hub_tickets_key)
    JOIN dwh_detailed.hub_flights hf ON (hf.hub_flights_key = ltf.hub_flights_key)
    JOIN dwh_detailed.sat_flights_detail sfd ON (hf.hub_flights_key = sfd.hub_flights_key)
    JOIN dwh_detailed.sat_ticket_flights_detail stfd ON (ltf.link_ticket_flights_key = stfd.link_ticket_flights_key)
    WHERE AGE('{{ logical_date | ds }}'::timestamptz, sfd.scheduled_arrival) <= INTERVAL '1 year' AND
          st.load_dts = (SELECT MAX(st2.load_dts) FROM dwh_detailed.sat_tickets_detail st2 WHERE st2.hub_tickets_key = st.hub_tickets_key) AND
          stfd.load_dts = (SELECT MAX(stfd2.load_dts) FROM dwh_detailed.sat_ticket_flights_detail stfd2 WHERE stfd2.link_ticket_flights_key = stfd.link_ticket_flights_key) AND
          sfd.load_dts = (SELECT MAX(sfd2.load_dts) FROM dwh_detailed.sat_flights_detail sfd2 WHERE sfd2.hub_flights_key = sfd.hub_flights_key)
    GROUP BY st.passenger_id),

    passenger_status AS(
    SELECT 
        pg1.passenger_id as passenger_id,
        CASE
            WHEN 20 * (SELECT COUNT(*) FROM passenger_gmv pg2 WHERE pg2.gmv >= pg1.gmv) <= (SELECT COUNT(*) FROM passenger_data) THEN '5'
            WHEN 10 * (SELECT COUNT(*) FROM passenger_gmv pg2 WHERE pg2.gmv >= pg1.gmv) <= (SELECT COUNT(*) FROM passenger_data) THEN '10'
            WHEN 4 * (SELECT COUNT(*) FROM passenger_gmv pg2 WHERE pg2.gmv >= pg1.gmv) <= (SELECT COUNT(*) FROM passenger_data) THEN '25'
            WHEN 2 * (SELECT COUNT(*) FROM passenger_gmv pg2 WHERE pg2.gmv >= pg1.gmv) <= (SELECT COUNT(*) FROM passenger_data) THEN '50'
            WHEN 2 * (SELECT COUNT(*) FROM passenger_gmv pg2 WHERE pg2.gmv >= pg1.gmv) > (SELECT COUNT(*) FROM passenger_data) THEN '50+'
        END as customer_group
    FROM passenger_gmv pg1)

    SELECT
        '{{ logical_date | ts }}'::timestamptz as created_at,
        pd.passenger_id as passenger_id,
        pd.passenger_name as passenger_name,
        pd.flights_number as flights_number,
        pd.purchase_sum as purchase_sum,
        pam.airport_code as airport_code,
        COALESCE(ps.customer_group, '50+') as customer_group
    FROM passenger_data pd
    LEFT JOIN passenger_status ps ON (pd.passenger_id = ps.passenger_id)
    JOIN passenger_airport_max pam ON (pd.passenger_id = pam.passenger_id);
'''


sql_script_2 = '''
    CREATE SCHEMA IF NOT EXISTS presentation;

    CREATE TABLE IF NOT EXISTS presentation.task2 (
        created_at timestamptz,
        flight_date timestamptz,
        airport_code text,
        linked_airport_code text,
        fights_in integer,
        fights_out integer,
        passengers_in integer,
        passengers_out integer
    );

    DELETE FROM presentation.task2
    WHERE flight_date = '{{ yesterday_ds }}'::timestamptz;

    WITH main_arrival_airport_data AS(
    SELECT
        ha.airport_code as airport_code,
        l_ha.airport_code as linked_airport_code,
        COUNT(DISTINCT hf.hub_flights_key) as flights_in,
        COUNT(DISTINCT ltf.link_ticket_flights_key) as passengers_in
    FROM dwh_detailed.hub_airports ha
    CROSS JOIN dwh_detailed.hub_airports l_ha
    JOIN dwh_detailed.link_flights_arrival_airport lfaa ON (ha.hub_airports_key = lfaa.arrival_airport_key)
    JOIN dwh_detailed.link_flights_departure_airport l_lfda ON (l_ha.hub_airports_key = l_lfda.departure_airport_key)
    JOIN dwh_detailed.hub_flights hf ON (hf.hub_flights_key = l_lfda.hub_flights_key AND hf.hub_flights_key = lfaa.hub_flights_key)
    JOIN dwh_detailed.link_ticket_flights ltf ON (hf.hub_flights_key = ltf.hub_flights_key)
    JOIN dwh_detailed.sat_flights_detail sfd ON (hf.hub_flights_key = sfd.hub_flights_key)
    WHERE DATE(sfd.actual_departure) = DATE('{{ yesterday_ds }}'::timestamptz) AND
          sfd.load_dts = (SELECT MAX(sfd2.load_dts) FROM dwh_detailed.sat_flights_detail sfd2 WHERE sfd2.hub_flights_key = sfd.hub_flights_key AND DATE(sfd2.actual_departure) = DATE('{{ yesterday_ds }}'::timestamptz))
    GROUP BY ha.airport_code, l_ha.airport_code),

    main_departure_airport_data AS(
    SELECT
        ha.airport_code as airport_code,
        l_ha.airport_code as linked_airport_code,
        COUNT(DISTINCT hf.hub_flights_key) as flights_out,
        COUNT(DISTINCT ltf.link_ticket_flights_key) as passengers_out
    FROM dwh_detailed.hub_airports ha
    CROSS JOIN dwh_detailed.hub_airports l_ha
    JOIN dwh_detailed.link_flights_arrival_airport l_lfaa ON (l_ha.hub_airports_key = l_lfaa.arrival_airport_key)
    JOIN dwh_detailed.link_flights_departure_airport lfda ON (ha.hub_airports_key = lfda.departure_airport_key)
    JOIN dwh_detailed.hub_flights hf ON (hf.hub_flights_key = lfda.hub_flights_key AND hf.hub_flights_key = l_lfaa.hub_flights_key)
    JOIN dwh_detailed.link_ticket_flights ltf ON (hf.hub_flights_key = ltf.hub_flights_key)
    JOIN dwh_detailed.sat_flights_detail sfd ON (hf.hub_flights_key = sfd.hub_flights_key)
    WHERE DATE(sfd.actual_departure) = DATE('{{ yesterday_ds }}'::timestamptz) AND
      sfd.load_dts = (SELECT MAX(sfd2.load_dts) FROM dwh_detailed.sat_flights_detail sfd2 WHERE sfd2.hub_flights_key = sfd.hub_flights_key AND DATE(sfd2.actual_departure) = DATE('{{ yesterday_ds }}'::timestamptz))
    GROUP BY ha.airport_code, l_ha.airport_code)

    INSERT INTO presentation.task2
    SELECT
        '{{ logical_date | ts }}'::timestamptz as created_at,
        '{{ yesterday_ds }}'::timestamptz as flight_date,
        ha.airport_code as airport_code,
        l_ha.airport_code as linked_airport_code,
        COALESCE(maad.flights_in, 0) as flights_in,
        COALESCE(mdad.flights_out, 0) as flights_out,
        COALESCE(maad.passengers_in, 0) as passengers_in,
        COALESCE(mdad.passengers_out, 0) as passengers_out
    FROM dwh_detailed.hub_airports ha
    CROSS JOIN dwh_detailed.hub_airports l_ha
    LEFT JOIN main_arrival_airport_data maad ON (ha.airport_code = maad.airport_code AND l_ha.airport_code = maad.linked_airport_code)
    LEFT JOIN main_departure_airport_data mdad ON (ha.airport_code = mdad.airport_code AND l_ha.airport_code = mdad.linked_airport_code)
    WHERE ha.airport_code <> l_ha.airport_code;
'''


DEFAULT_ARGS = {
    'owner': 'ShompolovMaxim',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 13),
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=120)
}

# ----------------- ФУНКЦИИ ----------------

pass

# ------------------- КОД ------------------

with DAG("update_data_marts",
         default_args=DEFAULT_ARGS,
         catchup=False,
         schedule_interval="0 3 * * *",
         max_active_runs=1,
         is_paused_upon_creation=False,
         concurrency=1) as dag:
    
    task1 = PostgresOperator(
        task_id="task1",
        postgres_conn_id="greenplum_dwh_conn",
        sql=sql_script_1,
    )

    task2 = PostgresOperator(
        task_id="task2",
        postgres_conn_id="greenplum_dwh_conn",
        sql=sql_script_2,
    )

    task1 >> task2