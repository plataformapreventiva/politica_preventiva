import pandas as pd
import pdb
from dotenv import load_dotenv, find_dotenv
import sqlalchemy

from politica_preventiva.pipelines.utils.postgres_utils import connect_to_db, get_engine

load_dotenv(find_dotenv())


TIPOS_RECOMENDACION = {'clave': sqlalchemy.types.VARCHAR(8),
                       'nivel': sqlalchemy.types.VARCHAR(29),
                       'timestamp': sqlalchemy.types.TIMESTAMP,
                       'metadata': sqlalchemy.types.Text,
                       'periodo': sqlalchemy.types.TIMESTAMP,
                       'fuente': sqlalchemy.types.Text,
                       'valor': sqlalchemy.types.ARRAY(sqlalchemy.types.Text),
                       'tipo': sqlalchemy.types.Text,
                       'nombre': sqlalchemy.types.Text}


def get_row_value(row, df, column):
    if row['subindicador']:
        ids =df[df['subindicador']==df['subindicador']][column].iloc[0]
    elif row['indicadores']:
        ids = df[df['indicadores']==row['indicadores']][column].iloc[0]
    else:
        ids = None
    return ids


def get_values(df, column):
    query = """SELECT * FROM clean.riesgo_recomendacion"""
    con = connect_to_db()
    relaciones = pd.read_sql(query, con)
    riesgos = df.pivot_table(index='clave', columns='tipo', values='valor').copy()
    types_dimensions = relaciones.columns[1:-3]
    con.close()

    for i in range(len(types_dimensions)):
       if i == 0:
            son = types_dimensions[i]
            son_unicos = relaciones[son].unique()
            riesgos[son] = riesgos[son_unicos].idxmax(axis=1)
       else:
            parent = types_dimensions[i-1]
            parent_unicos = relaciones[parent].unique()
            son = types_dimensions[i]
            riesgos[son] = None
            for p in parent_unicos:
                son_unicos = relaciones[relaciones[parent]==p][son].unique()
                son_unicos = [x  for x in son_unicos if x in riesgos.columns]
                if len(son_unicos) > 0:
                    riesgos.loc[riesgos[parent]==p, son] = riesgos[riesgos[parent]==p][son_unicos].idxmax(axis=1)

    riesgos = riesgos[types_dimensions].copy()
    claves = pd.DataFrame(riesgos.apply(lambda x: get_row_value(x,relaciones, column), axis=1).reset_index())
    claves.columns = ['clave', 'valor']
    return claves


def add_sgds_to_table():
    query_riesgos = """SELECT * FROM semantic.riesgos"""
    con = connect_to_db()
    riesgos = pd.read_sql(query_riesgos, con)
    sdgs = get_values(riesgos, 'sdg_ids')
    riesgos = riesgos[['clave','nivel','timestamp','metadata','periodo','fuente']].drop_duplicates(['clave','nivel']).copy()
    riesgos['timestamp'] = riesgos['timestamp'].apply(lambda d: pd.to_datetime(str(d)))
    riesgos['periodo'] = riesgos['periodo'].apply(lambda d: pd.to_datetime(str(d)))
    riesgos = riesgos.merge(sdgs, on='clave', how='left')
    riesgos['tipo'] = 'recomendacion_sdgs'
    riesgos['nombre'] = 'Recomendación de Objetivo de Desarrollo Sostenible'
    engine=get_engine()
    riesgos.to_sql(name='recomendaciones', con=engine, schema='semantic', if_exists='replace',index=False, dtype=TIPOS_RECOMENDACION)
    con.close()


def add_programas_to_table():
    query_riesgos = """SELECT * FROM semantic.riesgos"""
    con = connect_to_db()
    riesgos = pd.read_sql(query_riesgos, con)
    programas = get_values(riesgos, 'programas_ids')
    riesgos = riesgos[['clave','nivel','timestamp','metadata','periodo','fuente']].drop_duplicates(['clave','nivel']).copy()
    riesgos['timestamp'] = riesgos['timestamp'].apply(lambda d: pd.to_datetime(str(d)))
    riesgos['periodo'] = riesgos['periodo'].apply(lambda d: pd.to_datetime(str(d)))
    riesgos = riesgos.merge(programas, on='clave', how='left')
    riesgos['tipo'] = 'recomendacion_programas'
    riesgos['nombre'] = 'Recomendación de Programas Sociales Federales'
    engine=get_engine()
    riesgos.to_sql(name='recomendaciones', con=engine, schema='semantic', if_exists='append',index=False, dtype=TIPOS_RECOMENDACION)
    con.close()


def to_sql_array(values):
    val = list(values)
    return '{' + ",".join('"{}"'.format(x) for x in val) + '}'


def lists_intersection(d): 
    lista = []
    if len(d) > 0:
        sets = [set(i) for i in d if i] 
        if len(sets) > 0:
            result = sets[0]
            for s in sets:
                result = result.intersection(s)
            lista = list(result)
    return lista


if __name__=='__main__':
    #add_sgds_to_table()
    add_programas_to_table()
    ##cve_union_programas()

