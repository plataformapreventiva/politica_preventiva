#!/usr/bin/env bash

#########################
####    EarthQuake  ####
#########################

# This script ingests earthquake data from
# https://earthquake.usgs.gov/fdsnws/event/1/

# Ahorita el classic_ingest_task nos manda el mes anterior.
# Para tener el reporte a fin de mes / inicio del siguiente.

year=$1
#year_t=$1-1
month=$2
#month_t=$2-1

local_path=$3
local_ingest_file=$4

echo 'Downloading Earthquake data'
URL="https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=$year-$month-01&endtime=$year-$month-31&minmagnitude=5&minlatitude=14.555427&maxlatitude=33.168382&minlongitude=-119.725409&maxlongitude=-85.316230" 

wget -qO- $URL | \
csvformat -D '|' > $local_ingest_file
