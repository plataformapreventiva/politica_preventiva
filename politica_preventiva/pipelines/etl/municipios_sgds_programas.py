import pandas as pd
import pdb
from dotenv import load_dotenv, find_dotenv

from politica_preventiva.pipelines.utils.postgres_utils import connect_to_db

load_dotenv(find_dotenv())


def case_statement(column_name, parent_name=None, parent_value=None):
    if parent_name:
        values = list(sdgs[sdgs[parent_name]==parent_value][column_name].unique())
    else:
        values = list(sdgs[column_name].unique())

    values = [x for x in values if x]
    if values:
        conditions = " ".join(["WHEN {value} THEN '{value}' ".format(value=v)
                        for v in values])
        query_case = ("""CASE greatest({inds}) 
                             {conditions}    
                         END  """.format(inds=", ".join(values),
                                         conditions=conditions))
        return query_case

def subqueries_statements(column_name, table, parent_name=None):
    if parent_name:
        parent_values = list(sdgs[parent_name].unique())

        when_cases = " ".join([" WHEN {parent_name} = '{parent}' THEN {case}"
                           .format(parent_name=parent_name,
                                   parent=parent,
                                   case=case_statement(column_name, parent_name=parent_name, parent_value=parent))
                                    for parent in parent_values
                                    if case_statement(column_name, parent_name=parent_name, parent_value=parent)])
        query_table = ("""{column} AS ( 
                        SELECT CASE 
                            {when_cases} 
                              END AS {column}, * 
                       FROM {parent_name})""".format(column=column_name,
                                                    when_cases=when_cases,
                                                    parent_name=parent_name))
    else:
        case = case_statement(column_name)
        query_table = (""" WITH {column} AS (
                          SELECT {case} as {column}, *
                          FROM semantic.{table}
                          )""".format(column=column_name,
                                      case=case,
                                      table=table))
                                      
    return query_table


def select_queries(types_dimensions, clave):
    last_dimension = types_dimensions[-1]
    query_selects = []
    for i in reversed(range(len(types_dimensions))):
        if i < len(types_dimensions) -1:
            condition = """WHERE r.{son} is NULL""".format(son=types_dimensions[i+1])
        else:
            condition = ""
        query_selects.append(""" SELECT {clave},
                                  {types_dimensions},
                                  r.sdg_ids
                            FROM {last}
                            JOIN clean.relacion_riesgo_sdg r
                         USING({i}) {condition}
                         """.format(clave=clave,
                                    types_dimensions=", ".join('r.{}'.format(x) for x in types_dimensions),
                                   last=last_dimension,
                                   i=types_dimensions[i],
                                   condition=condition))
    return query_selects

def to_sql_array(list_values):
    return '{' + ",".join('"{}"'.format(x) for x in list_values) + '}'

def add_sdg_to_table(table, clave):
    types_dimensions = sdgs.columns[1:-1]
    queries = []
    for i in range(len(types_dimensions)):
        print(i)
        if i == 0:
            queries.append(subqueries_statements(types_dimensions[i], table))
        else:
            queries.append(subqueries_statements(types_dimensions[i], table, types_dimensions[i-1]))

    selects = " UNION ".join(select_queries(types_dimensions, clave))
    
    whole_query = "{subqueries} {selects}".format(subqueries=", ".join(queries),
                                                  selects=selects)
    con = connect_to_db()
    df = pd.read_sql(whole_query, con)
    df['sdg_ids'] = df['sdg_ids'].apply(lambda x: to_sql_array(x))

    QUERY_ADD_COLUMN = ("""ALTER TABLE semantic.{table}
                           ADD COLUMN sdg_ids int[];""".format(table=table))
    cur = con.cursor()
    #cur.execute(QUERY_ADD_COLUMN)

    for d in range(len(df)):
        QUERY=(""" UPDATE semantic.{table}
                    SET sdg_ids = '{sdg_ids}'
                    WHERE semantic.{table}.{clave_col}='{clave_val}' """
                    .format(table=table,
                            sdg_ids = df['sdg_ids'].iloc[d],
                            clave_col=clave,
                            clave_val = df[clave].iloc[d]))
        cur.execute(QUERY)


def lists_intersection(*d):
    for di in d:
        for i in range(len(di)):
            s = set(di.iloc[i])
            if i == 0:
                res = s
            res = res.intersection(s)
    return res

def cve_union_programas(tabla, clave):
    query = ("""WITH unnested_programas AS (
                    SELECT n_sdg,
                           unnest(programas_sociales) AS programa
                    FROM clean.relacion_sdg_programas
                ), union_sdg AS (
                    SELECT n_sdg,
                           array_agg(DISTINCT programa) AS programas
                    FROM unnested_programas
                    GROUP BY 1
                ), unnested_claves AS (
                    SELECT {clave},
                           unnest(sdg_ids) AS n_sdg 
                    FROM semantic.{tabla}
                ) SELECT {clave},
                        programas
                  FROM unnested_claves
                  LEFT JOIN union_sdg
                  USING (n_sdg);""".format(tabla=tabla,
                                           clave=clave))
    con = connect_to_db()
    programas_df = pd.read_sql(query, con)
    df = pd.DataFrame(programas_df.groupby(clave)['programas'].apply(lists_intersection))
    df['programas'] = df['programas'].apply(lambda x: to_sql_array(x))
    df.reset_index(inplace=True)

    QUERY_ADD_COLUMN = ("""ALTER TABLE semantic.{table}
                           ADD COLUMN programas text[];""".format(table=tabla))
    cur = con.cursor()
    cur.execute(QUERY_ADD_COLUMN)
    for d in range(len(df)):
        QUERY=(""" UPDATE semantic.{table}
                   SET programas = '{programas}'
                   WHERE semantic.{table}.{clave_col}='{clave_val}' """
                   .format(table=tabla,
                           programas=df['programas'].iloc[d],
                           clave_col=clave,
                           clave_val=df[clave].iloc[d]))
        cur.execute(QUERY)


if __name__=='__main__':
    # Read relacion_riesgo-sdg
    query = """SELECT * FROM clean.relacion_riesgo_sdg"""
    con = connect_to_db()
    sdgs = pd.read_sql(query, con)
    add_sdg_to_table('r_municipios', 'cve_muni')
    add_sdg_to_table('r_estados', 'cve_ent')
    cve_union_programas('r_municipios', 'cve_muni')
    cve_union_programas('r_estados', 'cve_ent')


