#!/usr/bin/env bash

echo 'Downloading Lecherías'
#wget -qO-  https://s3-us-west-2.amazonaws.com/dpa-plataforma-preventiva/utils/data_temp/infraestructura/lecherias$1.csv | csvformat -D '|' > $3
wget -qO-  https://s3-us-west-2.amazonaws.com/dpa-plataforma-preventiva/utils/data_temp/infraestructura/lecherias2017-6.csv | csvformat -D '|' > $3
