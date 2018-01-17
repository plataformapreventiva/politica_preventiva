from __future__ import print_function
import argparse
import boto3
import re
import datetime
import os

from pyspark import SparkContext

D = {'cd_dependencia': 0,
     'nb_origen': 1,
     'id_admin': 2,
     'cd_programa': 3,
     'cd_padron': 4,
     'anio': 5,
     'periodo': 6,
     'nb_primer_ap': 7,
     'nb_segundo_ap': 8,
     'nb_nombre': 9,
     'fh_nacimiento': 10,
     'cd_sexo': 11,
     'nu_mes_pago': 12,
     'new_id': 13,
     'nu_imp_monetario': 14,
     'cve_ent': 15,
     'cve_mun': 16,
     'cve_loc': 17,
     'in_titular': 18,
     'cd_beneficio': 19}

def trim(string):
    try:
        return string.strip()
    except:
        return ""


def clean_column_names(x):
    x = x.strip()
    x = x.lower()
    return x


def to_age(birthdate, year, mes_corresp):
    today = datetime.date(year=year, day=1, month=mes_corresp)
    try:
        born = datetime.strptime(birthdate, '%Y%m%d')
        age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    except:
        age = None

    return age


def parse_age(age):
    try:
        if age >= 60:
            categoria = 'Adulto Mayor'
        elif age <= 14:
            categoria = 'Infante'
        elif age > 14:
            categoria = 'Adulto'
        else:
            categoria = None
    except:
        categoria = None
    return categoria

def clean_cve_muni(edo, muni):
    try:
        edo_int = int(edo)
        muni_int = int(muni)
    except:
        return None

    if (edo_int <= 32) & (edo_int > 0) & (muni_int < 999) & (muni_int > 0):
        cve_muni = "{ent}{mun}".format(ent=str(edo_int).zfill(2),
                                       mun=str(muni_int).zfill(4))
        return cve_muni

def clean_loc(loc):
    try:
        loc_int = int(loc)
    except:
        return None
    if (loc_int < 9999) & (loc_int > 0):
        cve_loc = str(loc_int).zfill(4)
        return cve_loc

def clean_cve_edo(edo):
    try:
        edo_int = int(edo)
    except:
        return None

    if (edo_int <= 32) & (edo_int > 0):
        cve_edo = str(edo_int).zfill(2)
        return cve_edo

def clean_gender(gender):
    try:
        if re.search('H', gender):
            gen = "H"
        elif re.search('M', gender):
            gen = "M"
        else:
            gen = None
    except:
        gen = None
    return gen

def clean_origin(nb_origin):
    try:
        if re.search('F', origin):
            origin = 'F'
        elif re.search('E', origin):
            origin = 'E'
        elif re.search('M', origin):
            origin = 'M'
        else:
            origin = None
    except:
        origin = None
    return origin

def clean_anio(anio):
   try:
        clean_anio = int(anio)
    except:
        clean_anio = None
    return clean_anio

def clean_mes(mes):
    try:
        clean_mes = int(mes)
    except:
        clean_mes = None
    if (clean_mes >= 1) & (clean_mes <= 12):
        return clean_mes

def gen_mes_corresp(periodo):
    mes = periodo[2]
    try:
        clean_mes = int(mes)
    except:
        if mes.lower() == 'a':
            clean_mes = 10
        elif mes.lower() == 'b':
            clean_mes = 11
        elif mes.lower() == 'c':
            clean_mes = 12
        else:
            clean_mes = None
    return clean_mes

def clean_pub(line):
    elems = line.split("|")

    # clean year and meses
    year = clean_anio(elems[D['anio']])
    nu_mes_pago = clean_mes(elems[D['nu_mes_pago']])
    mes_corresp = gen_mes_corresp(elems[D['periodo']])

    # generate age column and category
    edad = to_age(elems[D['fh_nacimiento']],
                  year,
                  mes_corresp)
    categoria_edad = parse_age(edad)

    # remove names for underage
    ap_pat = elems[D['nb_primer_ap']]
    ap_mat = elems[D['nb_segundo_ap']]
    nom = elems[D["nb_nombre"]]
    try:
        if edad < 18:
            ap_pat = ap_mat = nom = ""
    except:
        pass

    # clean clave muni
    cve_muni = clean_cve_muni(elems[D['cve_ent']],
                              elems[D['cve_mun']])
    cve_edo = clean_cve_edo(elems[D['cve_ent']])
    cve_loc = clean_loc(elems[D['cve_loc']])
    # clean gender
    gender = clean_gender(elems[D['cd_sexo']])

    # clean origin
    origin = clean_origin(elems[D['nb_origen']])

    result = [elems[D['cd_dependencia']],
              origin,
              elems[D['id_admin']],
              elems[D['cd_programa']],
              elems[D['cd_padron']],
              year,
              elems[D['periodo']],
              ap_pat,
              ap_mat,
              nom,
              elems[D['fh_nacimiento']],
              edad,
              categoria_edad,
              gender,
              nu_mes_pago,
              mes_corresp,
              elems[D['new_id']],
              elems[D['nu_imp_monetario']],
              cve_muni,
              cve_edo,
              cve_loc,
              elems[D['in_titular']],
              elems[D['cd_beneficio']]]
    return "|".join([str(x) if x else '' for x in result])


def read_pub(year, input_path, output_path):
    # read a raw text file from s3
    raw_data = sc.textFile(input_path + "pub_{0}.txt".format(year))
    return raw_data


if __name__ == "__main__":
    input_path = "s3n://pub-raw/"
    sc = SparkContext.getOrCreate()
    parser = argparse.ArgumentParser(description="S3 file combiner")
    parser.add_argument("--year", help="year", default = "2013")
    args = parser.parse_args()
    year = args.year
    print(year)
    output_path = "s3n://pub-raw/clean/{}".format(year)
    print(output_path)
    raw_df = read_pub(year, input_path, output_path)
    new_df = raw_df.map(clean_pub)
    new_df.saveAsTextFile(output_path)
