# !/bin/bash

# Descarga de Base de Datos Municipios con Alerta de Sequía
# Fuente Monitor de Sequía de Conagua
# http://smn.cna.gob.mx/es/climatologia/monitor-de-sequia/monitor-de-sequia-en-mexico
# Los archivos en Excel® contienen los municipios con al menos el 40% de su territorio afectado por alguna intensidad o condición de sequía desde D0 hasta D4.


curl -s  'http://smn.cna.gob.mx/tools/RESOURCES/Monitor%20de%20Sequia%20en%20Mexico/MunicipiosSequia.xlsx'\
	-H 'Accept-Encoding: gzip, deflate, sdch' \
	-H 'Accept-Language: es-MX,es;q=0.8,es-419;q=0.6,en;q=0.4' \
	-H 'Upgrade-Insecure-Requests: 1' \
	-H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36' \
	-H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' \
	-H 'Referer: http://smn.cna.gob.mx/es/climatologia/monitor-de-sequia/monitor-de-sequia-en-mexico' \
	-H 'Cookie: __utma=268696563.2086167031.1512739704.1512739704.1512742287.2; __utmc=268696563; __utmz=268696563.1512742287.2.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)'\
       	-H 'Connection: keep-alive' --compressed > $1/temp.xls
# Excel to csv
in2csv --no-inference temp.xls | csvformat -D "|" > $2.csv
# Clean temp
rm $1/temp.xls


