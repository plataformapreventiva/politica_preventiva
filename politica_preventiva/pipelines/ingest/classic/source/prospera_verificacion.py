import argparse
import boto3
import pandas as pd
from dbfread import DBF


def get_dataframe(bucket = '', key = '', download_path = ''):
    
    """ 
    Download dbf from S3 bucket as a dataframe.

    Args:
        bucket (str): string with name of the S3 bucket containing the file
        key (str): S3 file key
        download_path (str): local filename

    Returns:
        Pandas dataframe, if the file is found

    """
    
    s3 = boto3.client('s3')
    s3.download_file(Bucket = bucket, Key = key, Filename = download_path)
    dbf = DBF(download_path)
    data = pd.DataFrame(iter(dbf))
    return(data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Download PROSPERA verification data')
    parser.add_argument('-data_date', type = int, help = 'Year')
    parser.add_argument('-local_path', type = str, help = 'Local download path')
    parser.add_argument('-local_ingest_file', type = str, help = 'Local ingest file')
    args = parser.parse_args()
    _data_date = args.data_date
    _local_path = args.local_path
    _local_ingest_file = args.local_ingest_file
    
    key = 'prospera_' + str(_data_date) + '/identificacion/VERI.DBF'
    temp_download_path = 'prospera_verif_' + str(_data_date) + '.dbf'
    verif_data = get_dataframe(bucket = 'verificacion-raw', key = key, download_path = temp_download_path)

    verif_data.to_csv(_local_ingest_file, sep = '|', encoding = 'utf-8')

