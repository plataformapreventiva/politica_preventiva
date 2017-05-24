#!/bin/bash

echo 'Downloading INDESOl'

wget -O "$1/indesol.csv" "http://166.78.45.36/portal/organizaciones/excel/?cluni=&nombre=&acronimo=&rfc=&status_osc=&status_sancion=&figura_juridica=&estado=&municipio=&asentamiento=&cp=&rep_nombre=&rep_apaterno=&rep_amaterno=&num_notaria=&objeto_social=&red=&advanced="

echo 'Clean'
csvclean -d ','  -e 'latin1' -v $1/indesol.csv
#sed  "s/,,/,NA,/g; s/^,/NA,/g; s/,$/,NA/g; s/^N\/A,,/NA,/g; s/,N\/A,/,NA,/g; s/,N\/A$/,NA/g; s/|/-/g; s/,/|/g;" $1/indesol_out.csv > $2
