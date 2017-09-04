FROM python:3.5-onbuild

ADD requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

RUN mkdir /etc/luigi
ADD luigi.cfg /etc/luigi/luigi.cfg

RUN mkdir /var/log/luigid
ADD logrotate.cfg /etc/logrotate.d/luigid
VOLUME /var/log/luigid

RUN mkdir /var/run/luigid
VOLUME /var/run/luigid

CMD ["/usr/local/bin/luigid"]
EXPOSE 8083:8082

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
