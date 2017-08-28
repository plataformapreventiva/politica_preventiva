FROM python:3.5-slim 

MAINTAINER Roberto Sánchez Ávalos <r.sanchezavalos@gmail.com>

ENV LUIGI_HOME /etc/luigi

RUN apt-get update; \
	apt-get  -y update -yq && \
	apt-get  -y install build-essential curl unzip poppler-utils ssh git build-essential 

RUN apt-get -y install libpq-dev python-dev software-properties-common vim

# Docker

RUN apt-get -y install apt-transport-https ca-certificates &&\
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg |  apt-key add -&&\
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"  &&\
    apt-get update &&\
    apt-get install -y docker-ce &&\
    usermod -aG docker $(whoami) &&\
    service docker start

RUN curl -L \
	 https://github.com/docker/compose/releases/download/1.10.0/docker-compose-$(uname -s)-$(uname -m) \
	-o /usr/local/bin/docker-compose

RUN   apt-get --yes install libxss1 libappindicator1 libindicator7  && \
	apt-get  -y update -yq 

RUN chmod +x /usr/local/bin/docker-compose

#Docker-machine

RUN curl -L https://github.com/docker/machine/releases/download/v0.9.0/docker-machine-`uname -s`-`uname -m` \
	> /usr/local/bin/docker-machine && \
	chmod +x /usr/local/bin/docker-machine

RUN  apt install make

RUN apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
	libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
	xz-utils tk-dev

RUN apt-get update; \
	apt-get -y install golang-go &&\
	apt-get -y update &&\
	apt-get -y install silversearcher-ag &&\ 
	apt-get -y install python-pip

RUN cd &&\
	git clone git://github.com/yyuu/pyenv.git .pyenv &&\
	echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc &&\
	echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc &&\
	echo 'eval "$(pyenv init -)"' >> ~/.bashrc &&\
    /bin/bash -c "source ~/.bashrc"


RUN pip install --upgrade pip

# HUB
RUN wget https://github.com/github/hub/releases/download/v2.2.5/hub-linux-amd64-2.2.5.tgz &&\
	tar zvxvf hub-linux-amd64-2.2.5.tgz &&\
	 ./hub-linux-amd64-2.2.5/install

RUN service docker start
# Cleanup
RUN rm -rf hub-linux-amd64-2.2.5




ADD . /${PROJECT_NAME}/
WORKDIR /${PROJECT_NAME}


ADD requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

#RUN make init
#RUN make setup

CMD [ "/bin/sh", "-c", "while true; do echo hello world; sleep 1; done"]




