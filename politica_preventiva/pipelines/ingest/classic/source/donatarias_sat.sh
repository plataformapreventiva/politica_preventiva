#!/bin/bash

echo 'Downloading SAT donatarias table'

echo "$1"
if [ "$1" = "2017" ]
then
    echo '2017'
    URL='http://www.sat.gob.mx/terceros_autorizados/donatarias_donaciones/Documents/dir171.xls'
    wget -O "$2/sat_donatarias_$1.xls" $URL
    in2csv --no-inference "$2/sat_donatarias_$1.xls" | tail -n +31 | awk -F, '{$1= "2017";}1' OFS=, | sed '1s/$1/year/g' > "$2/tmp_$1.csv"
    csvformat -D "|" $2/tmp_$1.csv > $3
    rm "$2/sat_donatarias_$1.xls"
    rm "$2/tmp_$1.csv"

elif [ "$1" = "2015" ]
then
    echo '2015'
    URL='http://www.sat.gob.mx/terceros_autorizados/donatarias_donaciones/Documents/ddas15final.xls'
    wget -O "$2/sat_donatarias_$1.xls" $URL
    in2csv --no-inference "$2/sat_donatarias_$1.xls" | tail -n +31 | awk -F, '{$1= "2015";}1' OFS=, | sed '1s/$1/year/g' > "$2/tmp_$1.csv"
    csvformat -D "|" $2/tmp_$1.csv > $3
    rm "$2/sat_donatarias_$1.xls"
    rm "$2/tmp_$1.csv"
  
elif [ "$1" = "2014" ]
then
    echo '2014'
    URL='http://www.sat.gob.mx/terceros_autorizados/donatarias_donaciones/Documents/das1421515.xls'
    wget -O "$2/sat_donatarias_$1.xls" $URL
    in2csv --no-inference "$2/sat_donatarias_$1.xls" | tail -n +31 | awk -F, '{$1= "2014";}1' OFS=, | sed '1s/$1/year/g' > "$2/tmp_$1.csv"
    csvformat -D "|" $2/tmp_$1.csv > $3
    rm "$2/sat_donatarias_$1.xls"
    rm "$2/tmp_$1.csv"

else
    echo 'No information for that year'
    touch $3
    return
fi
