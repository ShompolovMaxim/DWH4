import os
import time
import yaml
import datetime
import requests
from typing import Dict, Any

DEBEZIUM_URL = 'http://debezium:8083/connectors'

pg_connector_config = {
  "name": "pg-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    
    "database.hostname": "postgres_master",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname" : "postgres",
    "database.server.name": "pg-dev",
    
    "table.include.list": "public.(.*)",
    "heartbeat.interval.ms": "5000",
    "slot.name": "dbname_debezium",
    "publication.name": "dbname_publication",
      
    "topic.creation.default.cleanup.policy": "delete",
    "topic.creation.default.partitions": "1",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.retention.ms": "604800000",
    "topic.creation.enable": "true",
    "topic.prefix": "postgres"
  }
}

sink_connectors_info = {
    'postgres.public.bookings': 'book_ref',
    'postgres.public.tickets': 'ticket_no',
    'postgres.public.airports': 'airport_code',
    'postgres.public.aircrafts': 'aircraft_code',
    'postgres.public.flights': 'flight_id',
    'postgres.public.seats': 'aircraft_code,seat_no',
    'postgres.public.ticket_flights': 'ticket_no,flight_id',
    'postgres.public.boarding_passes': 'ticket_no,flight_id',
}

def add_connector(connector_dict: Dict[str, Any]) -> None:
    response = requests.post(
        DEBEZIUM_URL, 
        json=connector_dict
    )
    assert response.status_code < 400, response.text


def add_pg_connector() -> None:
    add_connector(pg_connector_config)


def add_sink_connectors() -> None:
    for topic, record_keys in sink_connectors_info.items():
        add_connector(
            {
            "name": topic.replace('.', '-'),
            "config": {
                "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
                "topics": topic,
                "connection.url": "jdbc:postgresql://greenplum_dwh:5432/postgres",
                "connection.username": "gpadmin",
                "connection.password": "gppass",
                "tasks.max":"1",
                "insert.mode": "upsert",
                "delete.enabled": "false",
                "primary.key.mode": "record_key",
                "primary.key.fields": record_keys,
                "schema.evolution": "basic"
                }
            }
        )

add_pg_connector()
add_sink_connectors()

time.sleep(15)
print('DMP started')

dmp_ran = 0

while True:
    with open('dbt/dbt_project.yml', 'r') as dbt_project:
        data = yaml.load(dbt_project, Loader=yaml.FullLoader)
    data['vars']['load_date'] = str( datetime.datetime.now())
    with open('dbt/dbt_project.yml', 'w') as dbt_project:
        dbt_project.write(yaml.dump(data))
    os.system('cd dbt && sh run.sh')
    dmp_ran += 1
    print("DMP run")
    time.sleep(15)
    if dmp_ran == 1:
        os.system("""psql -p 5432 -h greenplum_dwh -U gpadmin -d postgres <<-EOSQL
                        ALTER TABLE dwh_detailed.hub_aircrafts SET DISTRIBUTED REPLICATED;
                        ALTER TABLE dwh_detailed.hub_airports SET DISTRIBUTED REPLICATED;
                        ALTER TABLE dwh_detailed.hub_bookings SET DISTRIBUTED RANDOMLY;
                        ALTER TABLE dwh_detailed.hub_flights SET DISTRIBUTED RANDOMLY;
                        ALTER TABLE dwh_detailed.hub_seats SET DISTRIBUTED BY (aircraft_code);
                        ALTER TABLE dwh_detailed.hub_tickets SET DISTRIBUTED RANDOMLY;
                        ALTER TABLE dwh_detailed.link_flights_aircrafts SET DISTRIBUTED BY (hub_aircrafts_key);
                        ALTER TABLE dwh_detailed.link_flights_arrival_airport SET DISTRIBUTED BY (arrival_airport_key);
                        ALTER TABLE dwh_detailed.link_flights_departure_airport SET DISTRIBUTED BY (departure_airport_key);
                        ALTER TABLE dwh_detailed.link_seats_aircrafts SET DISTRIBUTED BY (hub_aircrafts_key);
                        ALTER TABLE dwh_detailed.link_ticket_flights SET DISTRIBUTED BY (hub_tickets_key);
                        ALTER TABLE dwh_detailed.link_tickets_bookings SET DISTRIBUTED BY (hub_bookings_key);
                        ALTER TABLE dwh_detailed.sat_aircrafts_detail SET DISTRIBUTED REPLICATED;
                        ALTER TABLE dwh_detailed.sat_airports_detail SET DISTRIBUTED REPLICATED;
                        ALTER TABLE dwh_detailed.sat_ticket_flights_boarding_passes SET DISTRIBUTED BY (boarding_no);
                        ALTER TABLE dwh_detailed.sat_tickets_detail SET DISTRIBUTED BY (passenger_name);""")
    time.sleep(15)