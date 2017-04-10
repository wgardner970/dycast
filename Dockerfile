FROM python:2.7

RUN apt-get update -y &&\
	apt-get upgrade -y &&\
	apt-get install -y dos2unix

## Install Postgresql client
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" >> /etc/apt/sources.list.d/pgdg.list &&\
	wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - &&\
	apt-get update -y &&\
	apt-get install -y postgresql-client-9.1

RUN apt-get update -y &&\
    apt-get install -y postgis

# ptvsd = for debugging with Visual Studio Code
RUN pip install psycopg2 ptvsd

ENV ZIKAST_PATH=/zikast
ENV ZIKAST_APP_PATH=${ZIKAST_PATH}/application
ENV PG_SHARE_PATH=/usr/local/pgsql/share
ENV DOCKER_DIR=/docker

COPY ./application ${ZIKAST_APP_PATH}

COPY docker/entrypoint.sh ${DOCKER_DIR}/entrypoint.sh
RUN	chmod -R 700 ${DOCKER_DIR} &&\
	dos2unix ${DOCKER_DIR}/*

WORKDIR ${ZIKAST_APP_PATH}