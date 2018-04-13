#!/usr/bin/env bash

#########################
####    EarthQuake  ####
#########################

# This script ingests earthquake data from
# https://earthquake.usgs.gov/fdsnws/event/1/

# Ahorita el classic_ingest_task nos manda el mes anterior.
# Para tener el reporte a fin de mes / inicio del siguiente.

year=$1
month=$2
day=$3

local_path=$4
local_ingest_file=$5

echo 'Downloading Earthquake data'
URL="https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime="$year"-"$month"-"$day"T00:00:01.330Z&endtime="$year"-"$month"-"$day"T24:00:00.330Z&minmagnitude=5&minlatitude=14.555427&maxlatitude=33.168382&minlongitude=-119.725409&maxlongitude=-85.316230&orderby=time"

wget -qO- $URL | \
csvformat -D '|' > $local_ingest_file
