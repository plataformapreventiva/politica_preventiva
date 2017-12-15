import urllib.request
import pandas as pd
import pdb
import argparse
import os
import re
import zipfile


def coneval_municipios_to_csv(url, local_path, local_ingest_file, data_date):
    file_name = 'Concentrado, indicadores de pobreza.xlsx'
    urllib.request.urlretrieve(url, os.path.join(local_path, 'municipios.zip'))
    with zipfile.ZipFile(os.path.join(local_path, 'municipios.zip'), "r") as z:
        coneval_file = z.namelist()[0]
        #coneval_file = [x for x in coneval_files if 'municipio' in x][0]
        outpath = local_path + '/'
        z.extract(coneval_file, outpath)
        coneval_file, file_ext = os.path.splitext(coneval_file)

    fname = local_path + '/' + coneval_file + file_ext
    coneval = pd.read_excel(fname, header=[0,1], sheetname=0, skiprows=4, skip_footer=11)
    coneval.dropna(axis=0, how='all', inplace=True)

    # Change column names of multiindex
    new_cols = []
    for col in coneval.columns:
        new_cols.append("_".join([x.split('*')[0] for x in col if 'Unnamed' not in x]))
    coneval.columns = new_cols
    # Column keys
    cols_keys = ['Clave de entidad', 'Entidad federativa', 'Clave de municipio', 'Municipio']
    # Keep only the year one's
    keep_columns = cols_keys + [x for x in coneval.columns if '{}'.format(data_date) in x]
    coneval = coneval[keep_columns].copy()
    # Strip column names
    coneval.columns = [re.sub('\\n|{}'.format(data_date),'', x) for x in coneval.columns]
    # Save document
    coneval.to_csv(local_ingest_file, sep='|', index=False)
    os.unlink(fname)
    os.unlink(os.path.join(local_path, 'municipios.zip'))

if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description= 'Download CONEVAL municipios')
    parser.add_argument('--data_date', type=int, help='Data date')
    parser.add_argument('--local_path', type=str, help='Local path')
    parser.add_argument('--local_ingest_file', type=str, help='Local ingest file')
    args = parser.parse_args()

    url = 'http://www.coneval.org.mx/Medicion/Documents/Pobreza_municipal/Anexo_estadistico.zip'
    if args.data_date in [2010, 2015]:
        coneval_municipios_to_csv(url, args.local_path, args.local_ingest_file, args.data_date)

