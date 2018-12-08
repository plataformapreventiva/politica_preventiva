#!/usr/bin/env bash

echo 'Downloading Estancias'

#wget -qO-  https://s3-us-west-2.amazonaws.com/dpa-plataforma-preventiva/utils/data_temp/infraestructura/estancias$1.csv | csvformat -D '|' > $3
wget -qO-  https://s3-us-west-2.amazonaws.com/dpa-plataforma-preventiva/utils/data_temp/infraestructura/estancias2017-6.csv | csvformat -D '|' > $3
