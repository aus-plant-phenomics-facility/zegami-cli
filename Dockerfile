FROM python:3.6-alpine
RUN apk update
RUN apk add gcc libc-dev g++ libffi-dev libxml2 unixodbc-dev mariadb-dev postgresql-dev

#RUN pip install zegami-cli[sql] psycopg2
RUN pip install psycopg2

COPY . /zegami-cli

RUN chmod +x /zegami-cli/setup.py
#RUN python /zegami-cli/setup.py
RUN pip install /zegami-cli
