echo "Clearing data"
rm -rf ../postgresql-rp/data/*
rm -rf ../postgresql-rp/data-slave/*
#rm -R ./dags ./logs ./plugins ./config
mkdir -p ./dags ./logs ./plugins ./config
echo "AIRFLOW_UID=$(id -u)" > .env
chmod 777 debezium-connector-jdbc
docker-compose down

docker-compose up -d  postgres_master

echo "Starting postgres_master node..."
sleep 10

echo "Prepare replica config..."
docker exec -it postgres_master sh /etc/postgresql/init-script/init.sh
echo "Restart master node"
docker-compose restart

sleep 10

docker-compose up -d
