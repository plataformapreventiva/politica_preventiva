#!/bin/bash

while getopts p:t:n option;
do
 case "${option}"
 in
 p) path=${OPTARG};;
 t) task=${OPTARG};;
 n) new=${OPTARG};;
 esac
done


if [ "$new" != "False" ]; then
  echo "true statement";
  head -n 1 $path | sed "s/^/$task \: /g" >> \
    ./pipelines/common/raw_headers.yaml;

  sort -u ./pipelines/common/raw_headers.yaml >  \
    ./pipelines/common/raw_headers.temp;

  mv  ./pipelines/common/raw_headers.temp \
   ./pipelines/common/raw_headers.yaml;

  tail -n +2 $path >> $path.temp &&  mv $path.temp $path;

else
  echo "else statement";
  # Check if header
  new_header=$(head -n 1 $path);
  old_header=$(cat ./pipelines/common/raw_schemas_temp.txt | \
      grep "ipc_ciudades :" | awk -F ":" '{print $2}');
  if [ "$new_header" == "$old_header" ]; then
      tail -n +2 $path >> $path.temp &&  mv $path.temp $path;
  else
      break
  fi
fi;

