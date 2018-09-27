
###############
# ENIGH
# 2010-2016 
###############

echo "Descarga ENIGH"

year=$1
local_path=$2
local_ingest_file=$3
array=folioviv,tipo_viv,mat_pared,mat_techos,mat_pisos,antiguedad,antigua_ne,cocina,cocina_dor,cuart_dorm,num_cuarto,disp_agua,dotac_agua,excusado,uso_compar,sanit_agua,drenaje,disp_elect,combustible,estufa_chi,eli_basura,tenencia,num_dueno1,hog_dueno1,num_dueno2,hog_dueno2,escrituras,tipo_finan,renta,estim_pago,pago_viv,pago_mesp,tipo_adqui,viv_usada,lavadero,fregadero,regadera,tinaco_azo,cisterna,pileta,medidor_luz,bomba_agua,tanque_gas,aire_acond,calefacc,tot_resid,tot_hom,tot_muj,tot_hog,ubica_geo,ageb,tam_loc,est_socio,est_dis,upm,factor_viv

echo $year
if [ "$year" = 2012 ]
then
    name_file=http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/2012/microdatos/ncv_viviendas_2012_concil_2010_csv.zip
    file=12
elif [ "$year" = 2014 ];
then
    name_file=http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/2014/microdatos/NCV_Vivi_2014_concil_2010_csv.zip
    file=14   
elif [ "$year" = 2016 ]
then
    name_file=http://www.beta.inegi.org.mx/contenidos/proyectos/enchogares/regulares/enigh/nc/2016/microdatos/enigh2016_ns_viviendas_csv.zip
    file=16
else
    #2010
    echo 'No information for that year'
    exit
fi

if [ "$year" = 2012 ]
then
    wget $name_file -O $local_path/viviendas_$year.zip
    unzip $local_path/viviendas_$year.zip -d $local_path/viviendas_$year
    array2=$array,num_focos,calentador
    csvcut -c $array2 $local_path/viviendas_$year/*.csv | csvformat -e 'latin1' -D '|' > $local_ingest_file
elif [ "$year" = 2014 ] || [ "$year" = 2016 ];
then
    echo $year
    wget $name_file -O $local_path/viviendas_$year.zip
    unzip $local_path/viviendas_$year.zip -d $local_path/viviendas_$year
    mv $local_path/viviendas_$year/*.csv $local_path/viviendas_$year/auxiliar.csv
    cd $local_path/viviendas_$year
    sed -i 's/\r$// ; 1 s/^\xef\xbb\xbf//' auxiliar.csv
    arrayaux=focos_inca,focos_ahor
    csvcut -c $arrayaux auxiliar.csv | awk -F, '{print $1","$2","$1+$2}' | awk -F, '{print $3}' | tail -n +2 > aux_focos.csv
    sed -i '1s/^/num_focos\n/' aux_focos.csv
    paste -d',' aux_focos.csv auxiliar.csv > auxiliar2.csv
    arrayaux2=calent_sol,calent_gas
    csvcut -c $arrayaux2 "auxiliar.csv" | awk -F, '{print $1","$2","$1+$2}' | awk -F, '{print $3}' | tail -n +2 > aux_calent.csv
    sed -i '1s/^/calentador\n/' aux_calent.csv
    paste -d',' aux_calent.csv auxiliar2.csv > auxiliar3.csv
    arrayaux3=$array,num_focos,calentador
    if [ "$year" = 2016 ]
    then
        sed -i '1 s/factor/factor_viv/' auxiliar3.csv
    fi
    csvcut -c $arrayaux3 "auxiliar3.csv" | csvformat -e 'latin1' -D '|' > $local_ingest_file
else
    #2010
    echo 'No information for that year'
    exit
fi

rm aux*
rm $local_path/viviendas_$year.zip
rm -R $local_path/viviendas_$year/
