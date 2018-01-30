#!/usr/bin/env bash

###############
# SIFODE
# Ingesta de datos
###############

echo "SIFODE"

# Downloads

aws s3 cp s3://sifode-raw $1/temp --recursive

unrar p $1/temp/CALIFICACION_39_9.rar | csvformat -d "^" -D "|" | awk 'NR > 8 { print }' | sed 's/  0\%//' | sed 's/\s//' | sed 's/\xEF\xBB\xBF//' | sed 's///' | sed 's/\s\s\s\s//' | sed 's/\s[[:digit:]]%//' | sed 's/[[:digit:]]%//' | sed 's/\s[0-9]//' | head -n -2 >> $2
