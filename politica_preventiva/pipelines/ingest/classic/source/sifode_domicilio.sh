#!/usr/bin/env bash

###############
# SIFODE
# Ingesta de datos
###############

echo "SIFODE DOMICILIO"

# Downloads

aws s3 cp s3://sifode-raw/ $1/temp/ --recursive

unrar -inul p $1/temp/CALIFICACION_39_9.rar | csvformat -d "^" -D "|" >> $2 #| sed 's/  0\%//;s/\s//;s/\xEF\xBB\xBF//;s///;s/\s\s\s\s//;s/\s[[:digit:]]%//;s/[[:digit:]]%//;s/\s[0-9]//' | head -n -2 >> $2

#unrar p $1/temp/CALIFICACION_39_9.rar | csvformat -d "^" -D "|" | awk 'NR > 8 { print }' | sed 's/  0\%//' | sed 's/\s//' | sed 's/\xEF\xBB\xBF//' | sed 's///' | sed 's/\s\s\s\s//' | sed 's/\s[[:digit:]]%//' | sed 's/[[:digit:]]%//' | sed 's/\s[0-9]//' | head -n -2 >> $2
