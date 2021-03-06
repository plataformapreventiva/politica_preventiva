###############
# Transparencia Presupuestaria
###############
#sudo apt-get install python-dev python-pip python-setuptools build-essential
#pip install csvkit
# csvformat -D "|" $1$a > $2

echo "Descarga cuenta publica"

# Get SHCP cookie
cookie=$(curl -c - 'http://transparenciapresupuestaria.gob.mx/es/PTP/Datos_Abiertos' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Connection: keep-alive' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: es-MX,es;q=0.8,es-419;q=0.6,en;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36' --compressed  | egrep -o 'cookiesession1(.*)' | sed 's/cookiesession1//g;s/ +//g' )

if [ $1 = '2017-3' ]; then 
  url='http://www.transparenciapresupuestaria.gob.mx/work/models/PTP/DatosAbiertos/Bases_de_datos_presupuesto/CSV/pef_ac01_avance_2017.csv' 
fi

curl $url \
 -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: es-MX,es;q=0.8,es-419;q=0.6,en;q=0.4' \
 -H 'Upgrade-Insecure-Requests: 1' \
 -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36' \
 -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' \
 -H 'Referer: http://transparenciapresupuestaria.gob.mx/es/PTP/Datos_Abiertos' \
 -H "Cookie: cookiesession1=$cookie; FGTServer=9DBC1A3C2BDE20F2D0C36FF963399D8AD920C883B29A3E4FC089365787D26EEC6C38848A8522C982F59636F584CA4D27F21B0196E44AED;_ga=GA1.3.1379666131.1497550130; _gid=GA1.3.1973187680.1498619493; __atuvc=17%7C25%2C2%7C26; __atuvs=59531e627721332d001" \
 -H 'Connection: keep-alive' --compressed   >> $3.temp

echo "Changing encoding"
# sed '1d' $2.temp > $2.temp2
iconv -f iso-8859-1 -t utf-8 $3.temp | csvformat -D "|" > $3
#head -n -2 $3.tempp > $3
rm $3.temp; #rm $3.tempp

