import urllib.request
import pandas as pd
import pdb
import argparse
import os
import zipfile


def coneval_municipios_to_csv(url, local_path, local_ingest_file):
    file_name = '3.3 Concentrado, indicadores de pobreza por municipio.xlsx'
    urllib.request.urlretrieve(url, os.path.join(local_path, 'municipios_2010.zip'))
    with zipfile.ZipFile(os.path.join(local_path, 'municipios_2010.zip'), "r") as z:
        coneval_files = z.namelist()
        coneval_file = [x for x in coneval_files if 'municipio' in x][0]
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
    coneval.to_csv(local_ingest_file, sep='|', index=False)
    os.unlink(fname)
    os.unlink(os.path.join(local_path, 'municipios_2010.zip'))

if __name__ == '__main__':
    # Get Arguments from bash.
    parser = argparse.ArgumentParser(description= 'Download CONEVAL municipios')
    parser.add_argument('--local_path', type=str, help = 'Local path')
    parser.add_argument('--local_ingest_file', type=str, help = 'Local ingest file')
    args = parser.parse_args()
    url = 'http://www.coneval.org.mx/Informes/Pobreza/Pobreza_municipal/Anexo_estadistico/Concentrado.zip'
    coneval_municipios_to_csv(url, args.local_path, args.local_ingest_file)

