FROM python:3.10-slim

WORKDIR /opt/dagster/app

RUN pip install \
    dagster \
    dagster-webserver \
    dagster-postgres

COPY . /opt/dagster/app

EXPOSE 3001
