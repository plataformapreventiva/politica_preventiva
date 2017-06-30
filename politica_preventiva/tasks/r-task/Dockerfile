FROM dpa/r:0.1

ENV PROJECT_NAME politica_preventiva


## Copiamos los scripts
ADD . /${PROJECT_NAME}/
WORKDIR /${PROJECT_NAME}

RUN mkdir /${PROJECT_NAME}/data

CMD [ "Rscript", "/politica_preventiva/script.R" ]
