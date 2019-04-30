FROM dpa/python:0.1

ENV PROJECT_NAME politica_preventiva

## Copiamos los scripts
ADD . /${PROJECT_NAME}/
WORKDIR /${PROJECT_NAME}

RUN echo 'deb http://http.us.debian.org/debian stable main contrib non-free' >> /etc/apt/sources.list.d/jessie.non-free.list
RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y wget gdal-bin unrar libiconv-hook1

# Install Node.js
RUN curl -sL https://deb.nodesource.com/setup_8.x | bash
RUN apt-get install --yes nodejs
RUN apt-get install -y npm
RUN node -v
RUN npm -v
RUN npm i -g nodemon
RUN nodemon -v

# Install ogr2ogr
RUN npm install ogr2ogr
#RUN mkdir /${PROJECT_NAME}/data

ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

CMD [ "/bin/bash" ]
## ENTRYPOINT [ "python", "/../script.py" ]

