#!/usr/bin/env bash

###############
# SIFODE
# Ingesta de datos
###############

echo "SIFODE"

# Downloads

aws s3 cp s3://sifode-raw $1/CALIFICACION_39_9.rar #--recursive

unrar p $1/temp/CALIFICACION_39_9.rar | csvformat -d "^" -D "|" | awk 'NR > 8 { print }' | sed 's/  0\%//;s/\s//;s/\xEF\xBB\xBF//;s///;s/\s\s\s\s//;s/\s[[:digit:]]%//;s/[[:digit:]]%//;s/\s[0-9]//' | head -n -2 >> $2
