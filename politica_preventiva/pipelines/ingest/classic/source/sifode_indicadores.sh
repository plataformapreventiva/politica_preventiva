#!/usr/bin/env bash

###############
# SIFODE
# Ingesta de datos
###############

echo "SIFODE"

# Downloads

aws s3 cp s3://sifode-raw $2/temp/ --recursive

unzip -p $1/temp/Indicadores_SIFODE_39_9_v1.zip | csvformat -d "^" -D "|" >> $3
