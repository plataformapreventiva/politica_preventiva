FROM dpa/r:0.1

ENV PROJECT_NAME politica_preventiva


## Copiamos los scripts
ADD . /${PROJECT_NAME}/
WORKDIR /${PROJECT_NAME}

RUN apt-get -y clean &&  apt-get -y update &&  apt-get -y upgrade

RUN \
    apt-get -y -qq update; apt-get upgrade -y -qq; \
    apt-get install --no-install-recommends -y  curl

# Intento de hack para poder tener algunas versiones más recientes de paquetes
# RUN \
#    printf '%s\n' "deb http://deb.debian.org/debian stretch main" \
#     "deb http://deb.debian.org/debian-security/ stretch/updates main" \
#     "deb http://deb.debian.org/debian stretch-updates main" >> /etc/apt/sources.list

# RUN \
#    cp apt_preferences.txt /etc/apt/preferences.d/temp_fix.pref

RUN apt-get update \
    && apt-get install -y libblas-dev liblapack-dev gfortran \
                       libpq-dev postgresql postgresql-contrib
#                       libudunits2-dev libgdal-dev


RUN ["Rscript", "requirements.R"]
