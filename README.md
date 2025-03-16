### Источники:
- материал семинаров

### Запуск:
`sh docker-init.sh` в корневой директории репозитория

### Проделанная работа:
- взят результат из ДЗ3
- добавлен контейнер с metabase
- выполнен вход в metabase
- созданы 2 дашборда в соответствии с заданием
- видео с демонстрацией: https://disk.yandex.ru/i/MvZJ6qMaB1wv1w

### Примечания:
- view из ДЗ1 оставлен, но может работать медленно (там 5 left join, не хватило времени доработать)
- система тестировалась и наверняка работает в WSL
- при переносе в другую wsl (на другой машине) возникала проблема перекодировки из doc в unix (doc2unix)
- данные, добавляемые при создании базы, не соответствуют тем, по которым составлялись дашборды, так как без сохранения дашбордов перезапускать систему было было бы неэффективно

### Работа выполнялась на 10 баллов:
1) 4 балла: поднят metabase

2) 7 баллов: реализован дашборд по аэропортам ("Аэропорты")

3) 10 баллов: реализован дашборд по пассажирам ("Пассажиры")

## SQL запросы для дашбордов:

### Аэропорты

1)

        select COUNT(distinct active_airports.hub_airports_key)
        from (
        (
            select lfaa.arrival_airport_key as hub_airports_key
            from dwh_detailed.sat_flights_detail sft
            join dwh_detailed.hub_flights hf on (sft.hub_flights_key = hf.hub_flights_key)
            join dwh_detailed.link_flights_arrival_airport lfaa on (hf.hub_flights_key = lfaa.hub_flights_key)
            where ((NOW() - sft.actual_arrival) <= INTERVAL '30 days') or 
                ((NOW() - sft.scheduled_arrival) <= INTERVAL '30 days')
        ) union (
            select lfda.departure_airport_key as hub_airports_key
            from dwh_detailed.sat_flights_detail sft
            join dwh_detailed.hub_flights hf on (sft.hub_flights_key = hf.hub_flights_key)
            join dwh_detailed.link_flights_departure_airport lfda on (hf.hub_flights_key = lfda.hub_flights_key)
            where ((NOW() - sft.actual_departure) <= INTERVAL '30 days') or
                ((NOW() - sft.scheduled_departure) <= INTERVAL '30 days')
        )
        ) active_airports;


2) 

        select sft.actual_departure::date as arrival_date, COUNT(distinct lfda.departure_airport_key) as active_airports_count
        from dwh_detailed.sat_flights_detail sft
        join dwh_detailed.hub_flights hf on (sft.hub_flights_key = hf.hub_flights_key)
        join dwh_detailed.link_flights_departure_airport lfda on (hf.hub_flights_key = lfda.hub_flights_key)
        where (NOW() - sft.actual_departure) <= INTERVAL '30 days'
        group by sft.actual_departure::date;

3) 

        select airports_tickets.airport, COUNT(distinct airports_tickets.ticket)
        from (
        (
            select ha.airport_code as airport, ltf.hub_tickets_key as ticket
            from dwh_detailed.link_flights_arrival_airport lfaa
            join dwh_detailed.hub_airports ha on (lfaa.arrival_airport_key = ha.hub_airports_key)
            join dwh_detailed.hub_flights hf on (hf.hub_flights_key = lfaa.hub_flights_key)
            join dwh_detailed.link_ticket_flights ltf on (hf.hub_flights_key = ltf.hub_flights_key)
        ) union (
            select ha.airport_code as airport, ltf.hub_tickets_key as ticket
            from dwh_detailed.link_flights_departure_airport lfda
            join dwh_detailed.hub_airports ha on (lfda.departure_airport_key = ha.hub_airports_key)
            join dwh_detailed.hub_flights hf on (hf.hub_flights_key = lfda.hub_flights_key)
            join dwh_detailed.link_ticket_flights ltf on (hf.hub_flights_key = ltf.hub_flights_key)
        )
        ) as airports_tickets
        group by airports_tickets.airport;

4)

        select airports_flights.airport, COUNT(distinct airports_flights.flight)
        from (
        (
            select lfaa.hub_flights_key as flight, ha.airport_code as airport
            from dwh_detailed.link_flights_arrival_airport lfaa
            join dwh_detailed.hub_airports ha on (lfaa.arrival_airport_key = ha.hub_airports_key)
        ) union (
            select lfda.hub_flights_key as flight, ha.airport_code as airport
            from dwh_detailed.link_flights_departure_airport lfda
            join dwh_detailed.hub_airports ha on (lfda.departure_airport_key = ha.hub_airports_key)
        )
        ) as airports_flights
        group by airports_flights.airport;

### Пассажиры

1) 

        select count(distinct st.passenger_id)
        from dwh_detailed.sat_tickets_detail st
        join dwh_detailed.hub_tickets ht on (st.hub_tickets_key = ht.hub_tickets_key)
        join dwh_detailed.link_ticket_flights ltf on (ht.hub_tickets_key = ltf.hub_tickets_key)
        join dwh_detailed.hub_flights hf on (hf.hub_flights_key = ltf.hub_flights_key)
        join dwh_detailed.sat_flights_detail sf on (sf.hub_flights_key = hf.hub_flights_key)
        where (NOW() - sf.actual_departure) <= INTERVAL '30 days';

2)

        select avg(total_amount)
        from dwh_detailed.sat_bookings_detail
        where (NOW() - book_date) <= INTERVAL '30 days';

3)

        select (count(hf.hub_flights_key) + 0.0) / count(distinct st.passenger_id)
        from dwh_detailed.sat_tickets_detail st
        join dwh_detailed.hub_tickets ht on (st.hub_tickets_key = ht.hub_tickets_key)
        join dwh_detailed.link_ticket_flights ltf on (ht.hub_tickets_key = ltf.hub_tickets_key)
        join dwh_detailed.hub_flights hf on (hf.hub_flights_key = ltf.hub_flights_key)
        join dwh_detailed.sat_flights_detail sf on (sf.hub_flights_key = hf.hub_flights_key)
        where (NOW() - sf.actual_departure) <= INTERVAL '30 days';

4)

        select sf.actual_departure::date, count(distinct st.passenger_id)
        from dwh_detailed.sat_tickets_detail st
        join dwh_detailed.hub_tickets ht on (st.hub_tickets_key = ht.hub_tickets_key)
        join dwh_detailed.link_ticket_flights ltf on (ht.hub_tickets_key = ltf.hub_tickets_key)
        join dwh_detailed.hub_flights hf on (hf.hub_flights_key = ltf.hub_flights_key)
        join dwh_detailed.sat_flights_detail sf on (sf.hub_flights_key = hf.hub_flights_key)
        group by sf.actual_departure::date;

5)

        select book_date::date, sum(total_amount)
        from dwh_detailed.sat_bookings_detail
        group by book_date::date;

6)

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
        WHERE AGE(NOW()::timestamptz, sfd.scheduled_arrival) <= INTERVAL '1 year' AND
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

        SELECT ps.customer_group, COUNT(ps.passenger_id)
        FROM passenger_status ps
        GROUP BY ps.customer_group;
