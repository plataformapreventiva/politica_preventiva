""" 
Load Sedesol data into an existing schema in a postgres database.
	This is a commandline script that can be used to ingest all the raw data from
	[Plataforma Preventiva] Pipeline Sedesol/local raw into a Postgres RDS schema. 

"""

########
# ¿ Define Connection ? 
# This is not going to be necessary as we are going to set up an 
# ami Role for our EC2. 
########

# Environment variables
# PGUSER; PGPASSWORD; PGHOST:

# AWS-S3
# Define Path  
# Define the location of the RAW files

bucket="sedesol-raw"
year=`date +'%Y'`
########

########
# Load PUB csv's
########

# Raw PUB_Muncipal
$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGHOST/predictivadb -c "DROP TABLE pub_municipios;" )
$( PGOPTIONS="--search_path=raw"  csvsql --db postgresql://$PGUSER:$PGPASSWORD@$PGHOST/predictivadb --no-inference --table pub_municipios --insert pub_municipios.csv )

# Raw PUB_Estatal 
$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGHOST/predictivadb -c "DROP TABLE raw.pub_estados;" )
$( PGOPTIONS="--search_path=raw"  csvsql --db postgresql://$PGUSER:$PGPASSWORD@$PGHOST/predictivadb --no-inference --table pub_estados --insert pub_estados.csv )

# ## create raw dic
$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGHOST/predictivadb -c 'DROP TABLE pub_diccionario_programas;' )
$( PGOPTIONS="--search_path=raw"  csvsql --db postgresql://$PGUSER:$PGPASSWORD@$PGHOST/predictivadb --table pub_diccionario_programas --insert pub_diccionario_programas.csv )


########
# INEGI
########


# Mortalidad [Defunciones Generales y Fetales]
#parallel curl -O  http://www.beta.inegi.org.mx/contenidos/proyectos/registros/vitales/mortalidad/microdatos/defunciones/[2013-$year]/defunciones_base_datos_[2013-$year]_csv.zip
#| aws s3 cp - s3://$bucket/inegi/defunciones_fetales/ 
#wget -qO 'http://www.beta.inegi.org.mx/contenidos/proyectos/registros/vitales/mortalidad/microdatos/fetales/'$i'/fetales_base_de_datos_'$i'_csv.zip' | aws s3 cp - s3://$bucket/inegi/defunciones_generales/ 
#done



