FROM python:3.6-alpine
RUN apk update
RUN apk add gcc libc-dev g++ libffi-dev libxml2 unixodbc-dev mariadb-dev postgresql-dev

RUN pip install zegami-cli[sql] psycopg2

#RUN mkdir -p /root/.local/share/zegami-cli

#WORKDIR /root

#ENTRYPOINT ["zeg"]
