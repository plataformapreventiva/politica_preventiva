""" 
Load Sedesol data into an existing schema in a postgres database.
	This is a commandline script that can be used to ingest all the raw data from
	[Plataforma Preventiva] Pipeline Sedesol/local raw into a Postgres RDS schema. 

	Asume que las bases ya están en S3
"""

########
# Define the location of the RAW files
bucket=s3://sedesol-raw
year=`date +'%Y'`
########


########
# Function
########

S3_CSV_RDS ()
# Environment variables
# PGUSER; PGPASSWORD; PGHOST:
# s3_2_RDS $s3bucket $db $schema $table  
{
PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGHOST/$2 -c \
"DROP TABLE $3.$4;"

aws s3 cp $bucket/$1/$4.csv - |  PGOPTIONS="--search_path=$3"  \
 csvsql --db postgresql://$PGUSER:$PGPASSWORD@$PGHOST/$2 \
 --table $4 --insert 
}


########
# Coneval /datos/coneval/municipal
########

S3_CSV_RDS coneval predictivadb raw coneval_municipios_dic 



########
# Load PUB csv's
########

# Raw PUB_Diccionario
S3_CSV_RDS pub predictivadb raw pub_diccionario_programas

# Raw PUB_Municipal 
S3_CSV_RDS pub predictivadb raw pub_municipios 
# Raw PUB_Estatal 
S3_CSV_RDS pub predictivadb raw pub_estados


########
# INEGI
########


# Mortalidad [Defunciones Generales y Fetales]
#parallel curl -O  http://www.beta.inegi.org.mx/contenidos/proyectos/registros/vitales/mortalidad/microdatos/defunciones/[2013-$year]/defunciones_base_datos_[2013-$year]_csv.zip
#| aws s3 cp - s3://$bucket/inegi/defunciones_fetales/ 
#wget -qO 'http://www.beta.inegi.org.mx/contenidos/proyectos/registros/vitales/mortalidad/microdatos/fetales/'$i'/fetales_base_de_datos_'$i'_csv.zip' | aws s3 cp - s3://$bucket/inegi/defunciones_generales/ 
#done

