FROM dpa/python:0.1

ENV PROJECT_NAME politica_preventiva
# ARG token
# ARG path
# ENV token=$token
# ENV path=$path

## Copiamos los scripts
# ADD . /${PROJECT_NAME}/
# WORKDIR /${PROJECT_NAME}

RUN apt-get -y install wget git

#RUN apt-get -y install wget nodejs npm gdal-bin
#RUN npm install ogr2ogr

ADD requirements.txt /tmp/requirements.txt
#RUN pip install -r /tmp/requirements.txt

# Copy ssh
RUN mkdir .ssh
# Primera opcion
# COPY ["id_sedesol", ".ssh/id_rsa"]
# Opción desde parámtreo
# RUN echo $ssh_prv_key > .ssh/id_rsa && \ 
#   chmod 600 .ssh/id_rsa
# RUN eval "$(ssh-agent -s)"
# RUN ssh-add .ssh/id_rsa


# COPY ["gitInit.sh", "/gitInit.sh"]
# RUN chmod +x /gitInit.sh
# ENTRYPOINT ["/gitInit.sh"]
# CMD [ git clone git@github.com:plataformapreventiva/politica_preventiva.git ] 
COPY entrypoint-script.sh /tmp/entrypoint-script.sh

ENTRYPOINT [ "/tmp/entrypoint-script.sh" ]
CMD [ "/bin/bash" ]