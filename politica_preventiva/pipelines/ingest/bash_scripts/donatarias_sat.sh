#!/bin/bash

echo 'Downloading SAT donatarias table'

if [ "$1" == "2017" ]
then
    echo '2017'
    URL='http://www.sat.gob.mx/terceros_autorizados/donatarias_donaciones/Documents/dir171.xls'

elif [ "$1" == "2015" ]
then
    echo '2015'
    URL='http://www.sat.gob.mx/terceros_autorizados/donatarias_donaciones/Documents/ddas15final.xls'

elif [ "$1" = "2014" ]
then
    echo '2014'
    URL='http://www.sat.gob.mx/terceros_autorizados/donatarias_donaciones/Documents/das1421515.xls'
else
    echo 'No information for that year'
    return 
fi

wget -O "$2/sat_donatarias.xls" $URL
in2csv --no-inference "$2/sat_donatarias.xls" | tail -n +31 | awk -F, '{$1="2017";}1' OFS=,i | sed '1s/2017/year/g' > "$2/tmp.csv"
csvformat -D "|" $LOCAL_RAW/tmp.csv > $3
rm "$2/sat_donatarias.xls"
rm "$2/tmp.csv"

