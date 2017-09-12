#!/bin/bash

###############
# CONEVAL
# Indicadores de pobreza, 2010-2014 (nacional y estatal), bases de microdatos finales
###############

echo "Descarga CONEVAL ESTADOS"

year=$1
local_path=$2
local_ingest_file=$3
array=proyecto,folioviv,foliohog,numren,tam_loc,rururb,ent,edad,sexo,tamhogesc,ic_rezedu,anac_e,inas_esc,niv_ed,ic_asalud,ic_segsoc,ss_dir,pea,par,jef_ss,cony_ss,hijo_ss,s_salud,pam,ic_cv,icv_hac,ic_sbv,isb_combus,ic_ali,id_men,tot_iaad,tot_iamen,ins_ali,plb_m,plb,ictpc,i_privacion,pobreza,pobreza_e,pobreza_m,vul_car,vul_ing,no_pobv,carencias,carencias3,cuadrantes,prof_b1,prof_bm1,profun,int_pob,int_pobe,int_vulcar,int_caren,ict


echo $year
if [ "$year" = 2010 ]
then
    name_file=R_2010
    file=10

elif [ "$year" = 2012 ]
then
    name_file=R_2012
    file=12
elif [ "$year" = 2014 ]
then
    name_file=R_2014
    file=14
elif [ "$year" = 2016 ]
then
    curl https://s3-us-west-2.amazonaws.com/dpa-plataforma-preventiva/utils/data_temp/pobreza_16.csv | sed 's/^/1\,/g' | sed '1 s/1/proyecto\,/'  | tr '[:upper:]' '[:lower:]' |\
	    csvcut -c $array | csvformat -D '|'  >  $local_ingest_file
    echo 'temporal ingestion'
    exit

else
    echo 'No information for that year'
    exit
fi 

curl http://www.coneval.org.mx/Medicion/MP/Documents/Programas_calculo_pobreza_10_12_14/${name_file}.zip > $local_path/coneval_$year.zip
unzip  $local_path/coneval_$year.zip -d $local_path/$year/
cat ${local_path}/$year/Base\ final/pobreza_$file.csv | tr '[:upper:]' '[:lower:]' |\
       	csvcut  -c $array | csvformat -e 'latin1' -D '|'  >  $local_ingest_file 

# rm $local_path/coneval_$year.zip
# rm -R $local_path/$year/
