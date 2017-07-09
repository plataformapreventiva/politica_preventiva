import boto3
import botocore
import logging
import os
import time
import yaml

from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv())


from politica_preventiva.pipelines.utils.emr_tasks import *

aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')



with open("pipelines/ingest/common/emr-config.yaml", "r") as file:
    config = yaml.load(file)
config_emr = config.get("emr")


emr_loader = EMRLoader(

    aws_access_key=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=config_emr.get("region_name"),
    cluster_name=config_emr.get("cluster_name"),
    instance_count=config_emr.get("instance_count"),
    master_instance_type=config_emr.get("master_instance_type"),
    slave_instance_type=config_emr.get("slave_instance_type"),
    key_name=config_emr.get("key_name"),
    subnet_id=config_emr.get("subnet_id"),
    log_uri=config_emr.get("log_uri"),
    software_version=config_emr.get("software_version"),
    script_bucket_name=config_emr.get("script_bucket_name"),
    key=config_emr.get("key"),
    key_value=config_emr.get("key_value")

)


# logger.info("*********************************i*********************")
# logger.info("Check if bucket exists otherwise create it and upload files to S3.")
# emr_loader.create_bucket_on_s3(bucket_name=config_emr.get("script_bucket_name"))
# emr_loader.upload_to_s3("scripts/bootstrap_actions.sh", 
# bucket_name=config_emr.get("script_bucket_name"),
#                    key_name="bootstrap_actions.sh")
# emr_loader.upload_to_s3("scripts/pyspark_quick_setup.sh", 
# bucket_name=config_emr.get("script_bucket_name"),
#                    key_name="pyspark_quick_setup.sh")


# logger.info( "*******************************************************")
# logger.info("Create cluster and run boostrap.")

emr_response = emr_loader.load_cluster()
emr_client = emr_loader.boto_client("emr")


