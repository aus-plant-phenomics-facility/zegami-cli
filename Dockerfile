FROM python:3.6-alpine
RUN apk update
RUN apk add gcc libc-dev g++ libffi-dev libxml2 unixodbc-dev mariadb-dev postgresql-dev

RUN pip install psycopg2
RUN pip install sqlalchemy

COPY . /zegami-cli

RUN pip install /zegami-cli
