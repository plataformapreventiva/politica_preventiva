#!/usr/bin/env bash

#########################
####    EarthQuake  ####
#########################

# This script ingest earthquake data from
# https://earthquake.usgs.gov/fdsnws/event/1/

period=$1
local_path=$2
local_ingest_file=$3

echo 'Downloading Earthquake data'

wget -qO-
https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2014-01-01&endtime=2014-01-02&minmagnitude=5&minlatitude=14.555427&maxlatitude=33.168382&minlongitude=-119.725409&maxlongitude=-85.316230 |\
 csvformat -D '|' > $3
