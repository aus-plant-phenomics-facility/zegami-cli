#FROM python:3.6-alpine3.7

#RUN apk add --no-cache --update \
#    python3 python3-dev gcc \
#    gfortran musl-dev g++ \
#    libffi-dev \
#    libxml2 libxml2-dev \
#    libxslt libxslt-dev \
#    libjpeg-turbo-dev zlib-dev postgresql-dev
#
#RUN pip install --upgrade pip

FROM amancevice/pandas:alpine

RUN apk update
RUN apk add gcc libc-dev g++ libffi-dev libxml2 unixodbc-dev mariadb-dev postgresql-dev


#RUN apk update

RUN pip3 install psycopg2
RUN pip3 install sqlalchemy
RUN pip3 install pandas

COPY . /zegami-cli

RUN pip3 install /zegami-cli

WORKDIR "/zegami-cli/appf-collection-builder"

ENTRYPOINT ["python3","main.py"]
