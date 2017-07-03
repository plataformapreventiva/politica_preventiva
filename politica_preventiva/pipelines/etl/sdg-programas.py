import re
import pandas as pd
import unicodedata
import pdb
import boto3
from dotenv import load_dotenv, find_dotenv
import os

from politica_preventiva.pipelines.utils.postgres_utils import connect_to_db
from politica_preventiva.pipelines.utils.pipeline_utils import s3_to_pandas

load_dotenv(find_dotenv())

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

def clean_strings(var_string):
    if isinstance(var_string, str):
        var_string = re.sub(r'[^\w\s]','',var_string)
        sub_string = " ".join(re.findall("[a-zA-Z]+", var_string))
        return sub_string.strip()

def clean_and_lower(var_string):
    var_string = strip_accents(var_string)
    if isinstance(var_string, str):
        return clean_strings(var_string).lower()

def clean_clave_edo(x):
    if isinstance(x, str):
        if x == '30 Ignacio de la Llave':
            return '30'
        elif 'Distrito' in x:
            return '32'
    else:
        if (x > 0) and (x <= 32):
            return str(x).zfill(2)

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def match(pattern, x):
    s = re.search(pattern, x)
    if s:
        return True
    else:
        return False

def return_matches(x):
    l = []
    for i in range(len(palabras_df)):
        ind = palabras_df.iloc[i]
        for pal in ind['palabras_clave']:
            if (match(pal, x['nombre_programa_clean'].iloc[0])) or (match(pal, x['objetivo_clean'].iloc[0])):
                l.append((ind['id']))
                                                                        
    return pd.DataFrame(l, columns=['id_palabra'])


def return_matches_list(x):
    l = {}
    j = []
    for i in range(len(palabras_df)):
        ind = palabras_df.iloc[i]
        for pal in ind['Palabras']:
            if (match(pal, x['nombre_programa_clean'])) or (match(pal, x['objetivo_clean'])):
                l[ind['Carencia']] = 1   
                j.append(ind['Subcarencia'])
    return l, j

def to_sql_array(values):
    val = list(values)
    return '{' + ",".join('"{}"'.format(x) for x in val) + '}'


if __name__ == "__main__":
    # Read BAse Programas
    bucket = 'dpa-plataforma-preventiva'
    s3_file = 'utils/BaseProgramas.csv'
    programas = s3_to_pandas(Bucket=bucket, Key=s3_file)
    # clean programas nombre and objetivo
    programas['nombre_programa_clean'] = programas['Nombre del programa / intervención'].map(clean_and_lower)
    programas['objetivo_clean'] = programas['Objetivo'].map(clean_and_lower)

    # Read palabras clave
    query = """select * from clean.relacion_sdg_programas"""
    con = connect_to_db()
    palabras_df = pd.read_sql(query, con)

    matches = (programas.groupby('ID').apply(lambda x: return_matches(x))).reset_index()
    matches = matches.groupby('id_palabra')['ID'].apply(to_sql_array)

    cur = con.cursor()
    for d in range(len(matches)):
        QUERY=(""" UPDATE clean.relacion_sdg_programas
                    SET programas_sociales= '{programas}'
                   WHERE clean.relacion_sdg_programas.id={id} """
                   .format(programas=matches.iloc[d],
                           id=matches.index[d]))
        cur.execute(QUERY)
