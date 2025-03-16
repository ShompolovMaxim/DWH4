CREATE VIEW extra_task_2 as select
a.airport_code as airport_code,
count(distinct f_d.flight_id) as departure_flights_num, 
count(distinct tf_d.ticket_no) as departure_psngr_num, 
count(distinct f_a.flight_id) as arrival_flights_num, 
count(distinct tf_a.ticket_no) as arrival_psngr_num
from airports a
left join flights f_a on (a.airport_code = f_a.arrival_airport)
left join flights f_d on (a.airport_code = f_d.departure_airport)
left join ticket_flights tf_a on (f_a.flight_id = tf_a.flight_id) 
left join ticket_flights tf_d on (f_d.flight_id = tf_d.flight_id) 
group by a.airport_code;
