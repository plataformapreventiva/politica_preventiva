FROM dpa/python:0.1

ENV PROJECT_NAME politica_preventiva

## Copiamos los scripts
ADD . /${PROJECT_NAME}/
WORKDIR /${PROJECT_NAME}

#RUN mkdir /${PROJECT_NAME}/data


ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt


CMD [ "/bin/bash" ]
## ENTRYPOINT [ "python", "/../script.py" ]

