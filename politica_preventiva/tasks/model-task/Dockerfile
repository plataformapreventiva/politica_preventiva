FROM politica_preventiva/task/r-task

ENV PROJECT_NAME politica_preventiva

RUN apt-get update && \
    apt-get upgrade && \
    apt-get install -y git

RUN apt-get -y install wget

# Install extra requirements
ADD requirements.txt /tmp/requirements.txt

# Copy script to clone model repository
COPY entrypoint-script.sh /tmp/entrypoint-script.sh

ENTRYPOINT [ "/tmp/entrypoint-script.sh" ]
