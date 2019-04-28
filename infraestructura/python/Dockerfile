FROM python:3.6-slim

MAINTAINER rsanchezavalos <r.sanchezavalos@gmail.com>

ENV REFRESHED_AT 2017-01-06

## Actualizamos
RUN apt-get -y clean &&  apt-get -y update &&  apt-get -y upgrade

RUN \
    apt-get -y -qq update; apt-get upgrade -y -qq; \
    apt-get install --no-install-recommends -y  curl locales bzip2 unzip xz-utils build-essential


RUN apt-get update \
    && apt-get install -y libblas-dev liblapack-dev gfortran \
                       libpq-dev \
                       make libssl-dev zlib1g-dev \
                       libbz2-dev libreadline-dev libsqlite3-dev llvm \
                       libncurses5-dev libncursesw5-dev xz-utils libpng-dev \
                       libfreetype6-dev libfreetype6 libxft-dev freetype* \
                       default-libmysqlclient-dev


ADD requirements.txt /tmp/requirements.txt
# RUN pip install -r /tmp/requirements.txt


CMD [ "/bin/bash" ]
