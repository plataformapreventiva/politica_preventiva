n#!/bin/bash

##############################
# CUIS VERIFICACION - ENCASEH
# Upload files to AWS S3
##############################

origdir=$HOME"/Verif_dom/Prospera_2015_2017"

cd "$origdir"

shopt -s globstar

for i in **/; do
	echo "$i";
    cd "$i";
    rename 'y/A-Z/a-z/' *.DBF;
    rename 'y/A-Z/a-z/' *.dbf;
    cd "$origdir";
done

cd "$HOME"

############
# 2017
############

# 2017 - Identificacion - 2017 - Formato_2016
aws s3 cp Verif_dom/Prospera_2015_2017/2017/2017_Prospera_10Ago17/20170804/Identificacion/Tablas/Formato_2016 s3://verificacion-raw/prospera_2017/Identificacion/Formato_2016/ --recursive

# 2017 - Identificacion - 2017 - Formato_2017
aws s3 cp Verif_dom/Prospera_2015_2017/2017/2017_Prospera_10Ago17/20170804/Identificacion/Tablas/Formato_2017 s3://verificacion-raw/prospera_2017/Identificacion/Formato_2017/ --recursive

# 2017 - Identificacion - 2017 - Resultados
aws s3 cp Verif_dom/Prospera_2015_2017/2017/2017_Prospera_10Ago17/20170804/Identificacion/Tablas/Resultados s3://verificacion-raw/prospera_2017/Identificacion/Resultados/ --recursive

# 2017 - Recertificacion - 2017 - Formato_2016
aws s3 cp Verif_dom/Prospera_2015_2017/2017/2017_Prospera_10Ago17/20170804/Recertificacion/Tablas/Formato_2016 s3://verificacion-raw/prospera_2017/Recertificacion/Formato_2016/ --recursive

############
# 2016
############

# 2016 - Identificacion
mv Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Identificacion/Tablas/Resultados/resultados_identificacion_2016.dbf Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Identificacion/Tablas/resultado_entrevista.dbf
rm -rf Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Identificacion/Tablas/Resultados
aws s3 cp Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Identificacion/Tablas s3://verificacion-raw/prospera_2016/Identificacion --recursive

# 2016 - Recertificacion
mv Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Recertificacion/Tablas/Resultados/resultados_recertificacion_2016.dbf Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Recertificacion/Tablas/resultado_entrevista.dbf
rm -rf Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Recertificacion/Tablas/Resultados
aws s3 cp Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Recertificacion/Tablas s3://verificacion-raw/prospera_2016/Recertificacion --recursive

# 2016 - Reevaluacion
mv Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Reevaluacion/Tablas/Resultados/resultados_reevaluacion.dbf Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Reevaluacion/Tablas/resultado_entrevista.dbf
rm Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Reevaluacion/Tablas/Resultados/
aws s3 cp Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/Reevaluacion/Tablas s3://verificacion-raw/prospera_2016/Reevaluacion/ --recursive

# 2016 - VPCS
mv Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/VPCS/Tablas/Resultados/resultados_vpcs.dbf Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/VPCS/Tablas/resultado_entrevista.dbf
rm -rf Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/VPCS/Tablas/Resultados/
aws s3 cp Verif_dom/Prospera_2015_2017/2016/2016_Prospera_17Feb17/VPCS/Tablas s3://verificacion-raw/prospera_2016/VPCS --recursive

############
# 2015
############

# 2015 - Identificacion
mv Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Identificacion/Tablas/Resultados/resultados_identificacion_230_232_235_236.dbf Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Identificacion/Tablas/resultado_entrevista.dbf
rm -rf Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Identificacion/Tablas/Resultados/
aws s3 cp Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Identificacion/Tablas s3://verificacion-raw/prospera_2015/Identificacion --recursive

# 2015 - Recertificacion
mv Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Recertificacion/Tablas/Resultados/resultado_recertificacion_231_237.dbf Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Recertificacion/Tablas/resultado_entrevista.dbf
rm -rf Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Recertificacion/Tablas/Resultados/
aws s3 cp Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Recertificacion/Tablas s3://verificacion-raw/prospera_2015/Recertificacion --recursive

# 2015 - Reevaluacion
mv Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Reevaluacion/Tablas/Resultados/resultados_reevaluacion_228_234.dbf Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Reevaluacion/Tablas/resultado_entrevista.dbf
rm -rf Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Reevaluacion/Tablas/Resultados/
aws s3 cp Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/Reevaluacion/Tablas s3://verificacion-raw/prospera_2015/Reevaluacion --recursive

# 2015 - VPCS
mv Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/VPCS/Tablas/Resultados/resultados_vpcs_229_233.dbf Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/VPCS/Tablas/resultado_entrevista.dbf
rm -rf Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/VPCS/Tablas/Resultados/
aws s3 cp Verif_dom/Prospera_2015_2017/2015/2015_Prospera_27Jun16/VPCS/Tablas s3://verificacion-raw/prospera_2015/VPCS --recursive

##############################
# Verificaciones 2011 - 2014
##############################

cd $HOME"/Verif_dom/Anexo_DGIGAE_1432_2017"
find | grep Veri > temp.txt
mkdir temp

IFS=$'\n'
set -f
for i in $(cat < "temp.txt"); do  
    path=$(dirname $i)
    name=$(echo $path | sed -e 's/\//_/g;s/^\._'//g)
	echo $name"_verif.dbf"
	cp $i temp/$name"_verif.dbf"
done

cd $HOME"/Verif_dom/Anexo_DGIGAE_1432_2017"
aws s3 cp ./temp s3://verificacion-raw/Verificaciones_2010_2014 --recursive
  