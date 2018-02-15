#!/usr/bin/env bash

###############
# SIFODE
# Ingesta de datos
###############

echo "SIFODE"

local_path=$1
local_ingest_file=$2

# Downloads
mkdir $local_path
aws s3 cp s3://sifode-raw $local_path/temp --recursive

tar -xOzf $local_path/temp/in_2014\_39_9.tar.gz | csvformat -d "^" -D "|" | awk '{ gsub(/\xef\xbb\xbf/,""); print }' > $local_ingest_file

yes | rm -r $local_path/temp
