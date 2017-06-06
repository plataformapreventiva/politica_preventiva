"""
	Preprocessing Functions
"""
import utils.preprocessing_utils as pputils
from luigi import configuration
import boto3 
from utils.pipeline_utils import s3_to_pandas, get_extra_str, pandas_to_s3, copy_s3_files
from io import StringIO
import pandas as pd
import boto3

def precios_granos_prep(year_month, s3_file, extra_h, out_key):
    """
    Preprocessing function for precios_granos: reads df from s3, completes missing values, 
    turns wide-format df to a long-format df, and uploads to s3
    """
    df = s3_to_pandas(Bucket='dpa-plataforma-preventiva', Key='etl/'+ s3_file)

    df['producto'] = pputils.complete_missing_values(df['producto'])
    columns = ['sem_1', 'sem_2', 'sem_3', 'sem_4', 'sem_5', 'prom_mes']
    df = pputils.gather(df, 'semana', 'precio', columns)
    df['semana'] = df['semana'].map(lambda x: x.replace('sem_', ''))

    pandas_to_s3(df, 'dpa-plataforma-preventiva', out_key)


def sagarpa_prep(year_month, s3_file, extra_h, out_key):
    """
    Preprocessing function for sagarpa: reads df from s3, completes missing values, 
    turns wide-format df to a long-format df, and uploads to s3
    """
    df = s3_to_pandas(Bucket='dpa-plataforma-preventiva', Key='etl/'+ s3_file)

    df['estado'] = pputils.complete_missing_values(df['estado'])
    df['distrito'] = pputils.complete_missing_values(df['distrito'])
    df['cultivo'] = extra_h 

    pandas_to_s3(df, 'dpa-plataforma-preventiva', out_key)

def inpc_prep(year_month, s3_file, extra_h, out_key):
    """
    Preprocessing function for inpc: reads df from s3, parses dates 
    and uploads to s3. 
    """   
    df = s3_to_pandas(Bucket='dpa-plataforma-preventiva', Key='etl/' + s3_file)

    df['month'] = df['fecha'].map(lambda x: pputils.inpc_month(x))
    df['year'] = df['fecha'].map(lambda x: pputils.inpc_year(x))
    
    pandas_to_s3(df, 'dpa-plataforma-preventiva', out_key)

def indesol_prep(year_month, s3_file, extra_h, out_key):
    """
    Preprocessing function for indesol: reads df from s3, turns wide-format df to long-format, 
    turns columns to json and uploads to s3. 
    """

    df = s3_to_pandas(Bucket='dpa-plataforma-preventiva', Key='etl/' + s3_file)

    # WIDE ACTIVIDAD TO LONG ACTIVDAD
    columns = ['ACTIVIDAD_' + str(x) for x in range(1,20)]
    df = pputils.gather(df, 'ACTIVIDAD', 'EDO_ACTIVIDAD', columns)
    df['ACTIVIDAD'] = df['ACTIVIDAD'].map(lambda x: x.replace('ACTIVIDAD_', ''))

    # RENAME INFORME COLUMNS 
    # TODO: CHANCE THIS TO REGEX
    # CHANGE STRINGS TO BOOL 
    columns = [x for x in df.columns if 'INFORME' in x]
    informe_dict = {col: col.replace('INFORME ', '') for col in columns}
    informe_dict = {key:informe_dict[key].replace(' EN TIEMPO', 'T') for key in informe_dict.keys()}
    informe_dict = {key:informe_dict[key].replace(' PRESENTADO', 'P') for key in informe_dict.keys()}
    df = df.rename(columns=informe_dict)    

    # TURN 'INFORME' COLUMNS TO JSON STRING
    columns = list(informe_dict.values())
    df = pputils.df_columns_to_json(df, columns, 'INFORMES')
    pandas_to_s3(df, 'dpa-plataforma-preventiva', out_key)

def cajeros_banxico_prep(year_month, s3_file, extra_h, out_key):
    bucket = 'dpa-plataforma-preventiva'
    copy_s3_files(bucket, 'etl' + s3_file, bucket, out_key)

def cenapred_prep(year_month, s3_file, extra_h, out_key):
    bucket = 'dpa-plataforma-preventiva'
    copy_s3_files(bucket, 'etl' + s3_file, bucket, out_key)

def segob_prep(year_month, s3_file, extra_h, out_key):
    bucket = 'dpa-plataforma-preventiva'
    copy_s3_files(bucket, 'etl' + s3_file, bucket, out_key)

def sagarpa_cierre_prep(year_month, s3_file, extra_h, out_key):
    # TODO: ver si es menor a 2013 (bajado como tabla completa, o mayor, y homologar columnas de ambos casos)
    bucket = 'dpa-plataforma-preventiva'
    copy_s3_files(bucket, 'etl' + s3_file, bucket, out_key)


"""

class inpc_preprocessing(luigi.Task):
    client = luigi.s3.S3Client()
    year_month = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    extra = luigi.Parameter()
    current_date = luigi.Parameter()
    raw_bucket = luigi.Parameter('DEFAULT')

    def run(self):
        # Read csv from s3
        df = s3_to_pandas(Bucket=self.raw_bucket, Key=self.pipeline_task + "/raw/" + \
            self.year_month + "--" +self.pipeline_task + extra_h + ".csv", sep="|")
        
        # Convert
        df['estado'] = pputils.complete_missing_values(df['estado'])
        df['distrito'] = pputils.complete_missing_values(df['distrito'])
        df['current_date'] = self.current_date
        
        # Convert df to string
        s = StringIO.StringIO()
        df.to_csv(s, sep='|')

        # s3 path
        extra_h = get_extra_str(self.extra)
        path = self.raw_bucket + self.pipeline_task + "/preprocess/" + \
            self.year_month + "--" +self.pipeline_task + extra_h + ".csv"
        
        return self.client.put_string(content=s.getvalue(), destination_s3_path = path)

    def output(self):

        # s3 path 
        extra_h = get_extra_str(self.extra)
        path = self.raw_bucket + self.pipeline_task + "/preprocess/" + \
            self.year_month + "--" +self.pipeline_task + extra_h + ".csv"

        return luigi.S3Target(self.raw_bucket + self.pipeline_task + "/preprocess/" + \
            self.year_month + "--" +self.pipeline_task + extra_h + ".csv")
"""