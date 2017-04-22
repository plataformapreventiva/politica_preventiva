# coding: utf-8
import subprocess
import os
from os.path import join, dirname
import luigi
from luigi.s3 import S3Target, S3Client
from luigi import configuration
from utils.pipeline_utils import parse_cfg_list

from dotenv import load_dotenv
## Variables de ambiente
path = os.path.abspath('__file__' + "/../../config/")
dotenv_path = join(path, '.env')
load_dotenv(dotenv_path)

aws_access_key_id =  os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


class bash_ingestion_s3(luigi.Task):
    pipeline_task = luigi.Parameter() # name of task, both scripts and csv will be stored this way
    year_month = luigi.Parameter() 

    client = luigi.s3.S3Client()

    bash_scripts =  luigi.Parameter() # path where scripts are located

    local_path =  luigi.Parameter() # path where csv is located 
    raw_bucket = luigi.Parameter()

    def run(self):
        #Guarda en temp
        bash_command =  self.bash_scripts + self.pipeline_task + '.sh '
        subprocess.call([bash_command], shell=True)

        return self.client.put(local_path=self.local_path + self.pipeline_task + ".csv",
            destination_s3_path=self.raw_bucket + self.pipeline_task + "/raw/" + self.year_month +"--" +self.pipeline_task + ".csv")

    def output(self):
        return S3Target(path=self.raw_bucket + self.pipeline_task + "/raw/" +  self.year_month +"--"+ self.pipeline_task + ".csv")

class python_ingestion_s3(luigi.Task):

    pipeline_task = luigi.Parameter()
    python_scripts = luigi.Parameter()
    year_month = luigi.Parameter() # year_month is the current date
    start_date = luigi.Parameter()

    client = luigi.s3.S3Client()

    local_path =  luigi.Parameter() # path where csv is located 
    raw_bucket = luigi.Parameter()

    def run(self):
        command_list = ['python', self.python_scripts + self.pipeline_task + ".py",
        '--start', self.start_date, self.year_month] 
        python_command = " ".join(command_list)
        subprocess.call([python_command], shell = True)

        dates = self.start_date + '_' + self.year_month
        destination_s3_path = self.raw_bucket + self.pipeline_task + "/raw/" + dates +"_"  + "maiz-grano.csv"

        return self.client.put(local_path=self.local_path + self.pipeline_task + '/' + dates + "_" + "maiz-grano.csv",
            destination_s3_path=destination_s3_path)
    
    def output(self):
        dates = self.start_date + '_' + self.year_month
        destination_s3_path = self.raw_bucket + self.pipeline_task + "/raw/" + dates +"_"  + "maiz-grano.csv"
        return S3Target(destination_s3_path)

#if __name__ == "__main__":
#    luigi.run()

# Get Riesgos from CENAPRED
#cenapred = get_cenapred_data()


# Get Precios from inegi
#INPC, metadata = get_inpc_ciudad_data()

#Download MSM shapefiles into s3bucket-local?
#get_smn_data(year='2016', location="local")