FROM dpa/python:0.1

ENV PROJECT_NAME politica_preventiva

## Copiamos los scripts
ADD . /${PROJECT_NAME}/
WORKDIR /${PROJECT_NAME}

RUN echo 'deb ftp://ftp.us.debian.org/debian/ jessie non-free deb http://security.debian.org/ jessie/updates non-free deb http://volatile.debian.org/debian-volatile jessie/volatile non-free' >> /etc/apt/sources.list.d/jessie.non-free.list
RUN apt-get update
RUN apt-get -y install wget nodejs npm gdal-bin unrar
RUN npm install ogr2ogr
#RUN mkdir /${PROJECT_NAME}/data


ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt


CMD [ "/bin/bash" ]
## ENTRYPOINT [ "python", "/../script.py" ]

