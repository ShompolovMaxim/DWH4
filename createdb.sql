create table bookings(
    book_ref char(6) primary key,
    book_date timestamptz not null,
    total_amount numeric(10,2) not null
);

create table tickets(
    ticket_no char(13) primary key,
    book_ref char(6) not null references bookings(book_ref),
    passenger_id varchar(20) not null,
    passenger_name text not null,
    contact_data jsonb
);

create table airports(
    airport_code char(3) primary key,
    airport_name text not null,
    city text not null,
    coordinates_lon double precision not null,
    coordinates_lat double precision not null,
    timezone text not null
);

create table aircrafts(
    aircraft_code char(3) primary key,
    model jsonb not null,
    range integer not null
);

create table flights(
    flight_id serial primary key,
    flight_no char(6) not null,
    scheduled_departure timestamptz not null,
    scheduled_arrival timestamptz not null,
    departure_airport char(3) not null references airports(airport_code),
    arrival_airport char(3) not null references airports(airport_code),
    status varchar(20) not null,
    aircraft_code char(3) not null references aircrafts(aircraft_code),
    actual_departure timestamptz,
    actual_arrival timestamptz
);

create table seats(
    aircraft_code char(3) references aircrafts(aircraft_code),
    seat_no varchar(4),
    fare_conditions varchar(10) not null,
    primary key (aircraft_code, seat_no)
);

create table ticket_flights(
    ticket_no char(13) references tickets(ticket_no),
    flight_id integer references flights(flight_id),
    fare_conditions numeric(10,2) not null,
    amount numeric(10,2) not null,
    primary key (ticket_no, flight_id)
);

create table boarding_passes(
    ticket_no char(13),
    flight_id integer,
    boarding_no integer not null,
    seat_no varchar(4) not null,
    primary key (ticket_no, flight_id),
    foreign key (ticket_no, flight_id) references ticket_flights(ticket_no, flight_id)
);

insert into bookings values ('12345a', '2024-10-19 10:23:54', 2);
insert into bookings values ('12345b', '2024-10-19 10:23:54', 2);
insert into bookings values ('12345c', '2024-10-19 10:23:54', 2);
insert into tickets values ('1234567890aaa', '12345a', 'a', 'name', '{"email" : "a@b.com"}');
insert into tickets values ('1234567890aab', '12345a', 'b', 'name2', '{"email" : "a@b.com"}');
insert into tickets values ('1234567890aac', '12345b', 'a', 'name', '{"email" : "a@b.com"}');
insert into tickets values ('1234567890aad', '12345c', 'b', 'name2', '{"email" : "a@b.com"}');
insert into airports values ('12a', 'airport', 'city', 1.2, 1.1, 'GMT');
insert into airports values ('12b', 'airport2', 'city2', -1.2, -1.1, 'GMT');
insert into aircrafts values ('12a', '{"model" : "a"}', 12);
insert into flights values (1, '12345a', '2024-10-22 10:23:54', '2024-10-22 13:23:54', '12a', '12b', 'arrived', '12a', '2025-03-15 10:23:54', '2024-10-22 13:23:54');
insert into flights values (2, '12345b', '2024-10-22 10:23:54', '2024-10-22 13:23:54', '12b', '12a', 'arrived', '12a', '2025-03-15 10:23:54', '2024-10-22 13:23:54');
insert into flights values (3, '12345c', '2022-10-22 10:23:54', '2022-10-22 13:23:54', '12a', '12b', 'arrived', '12a', '2022-10-22 10:23:54', '2022-10-22 13:23:54');
insert into ticket_flights values ('1234567890aaa', 1, 12, 13);
insert into ticket_flights values ('1234567890aab', 1, 12, 13);
insert into ticket_flights values ('1234567890aac', 2, 12, 13);
insert into ticket_flights values ('1234567890aad', 3, 12, 13);
insert into seats values('12a', 'a', 'a');
insert into boarding_passes values ('1234567890aaa', 1, 1, 'a');
insert into boarding_passes values ('1234567890aab', 1, 1, 'a');
insert into boarding_passes values ('1234567890aac', 2, 1, 'a');
insert into boarding_passes values ('1234567890aad', 3, 1, 'a');


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


