#! /bin/bash

""" 
Script para construir Isochrones, hace query a la API route360.
	input: 
		[1] pipeline Name
		[2] Array con latitud longitud
	output: Merged Geojson.

# To merge geojsons

# sudo apt-get update	Z
# sudo apt-get install nodejs
# sudo apt install npm 
# sudo npm install -g @mapbox/geojson-merge
# apt-get install nodejs-legacy
# geojson-merge file.geojson otherfile.geojson > combined.geojson
"""
echo "Petición de Isochrones para archivo $1"

eval $(cat ../config/.env | sed 's/^/export /')

apikey= $APIroute360

vartest=$( PGOPTIONS="--search_path=raw" \
        psql -t --db postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$PGHOST/predictivadb -c \
        "select lat,lon  from raw.cajeros_banxico;" | sed "s/| /,'lon':/g;s/^ /'lat':/g;"   )


# Create temporal directory
#sudo rm -rf ../data/isochrones/
mkdir ../data/isochrones/

# Hacerlo que corra en paralelo
# máximo 5000 peticiones
time_minutes=30
COUNTER=1; IFS=$'\n'; for i in $vartest
do
	echo $COUNTER " - 30 minutos"

	#URL=$( echo "https://service.route360.net/na_southwest/v1/polygon?cfg={'sources':[{$i \
	#,'id':'HG26SJ9SK','tm':{'car':{}}}],'polygon':{'serializer':'geojson','srid':'4326','values' \
	#:[600,1200]}}&key=TLMX3DHXOSG88L3T31SNK3Y" | sed -E "s/[[:space:]]+//g" )

	URL="http://matrix.mapzen.com/isochrone?json={'locations':[{${i}}],
	'costing':'auto','contours':[{'time':${time_minutes},'color':'ff0000'}]}&id=${COUNTER}&api_key=mapzen-97yjfaj&polygons=true"  
	
	URL=$(echo $URL | sed -E """s/[[:space:]]+//g;s/'/\"/g;""")
	curl --globoff "${URL}" -H 'Accept-Encoding: gzip, deflate, sdch'  -H 'Accept-Language: es-MX,es;q=0.8,es-419;q=0.6,en;q=0.4'  -H 'Upgrade-Insecure-Requests: 1'  -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'  -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'  -H 'Cache-Control: max-age=0'  -H 'Cookie: _ga=GA1.2.195185243.1492742379'  -H 'Connection: keep-alive' --compressed > ../data/isochrones/${i}_30.geojson
 	let COUNTER=COUNTER+1 
done



#for i in *; do
#  mv "$i" "`echo $i | sed "s/[\'|\:\|lat|lon|geojson]//g;s/\./_/g;s/ //g;s/\,/-/g;s/$/.geojson/g"`";
#done

geojson-merge /*.geojson > ../data/combined.geojson

mapshaper -i  combine-files \
    -merge-layers \
    -o pacific_states.shp

