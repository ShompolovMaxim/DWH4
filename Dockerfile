FROM ubuntu:22.04

ADD dbt dbt
ADD dmp.py dbt
RUN chmod 777 dbt/run.sh
RUN chmod 777 dbt/dbt_project.yml

RUN apt-get update
RUN apt-get -y install cron
RUN apt install python3 python3-pip python3-venv -y
RUN pip3 install dbt
RUN pip3 install dbt-postgres
RUN pip3 install requests
RUN apt-get -y install postgresql-client
RUN apt-get -y install git

RUN cd dbt && dbt deps
RUN cd ..

CMD python3 dbt/dmp.py