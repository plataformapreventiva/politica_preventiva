
echo 'Downloading Comedores'

# TODO install aws in docker
# aws s3 cp s3://dpa-plataforma-preventiva/utils/data_temp/infraestructura/comedores$1.csv $2/temp$1.csv

wget -qO-  https://s3-us-west-2.amazonaws.com/dpa-plataforma-preventiva/utils/data_temp/infraestructura/comedores2017-6.csv |\
       	csvformat -D '|' | awk -F'|' 'NF<70'  > $3
