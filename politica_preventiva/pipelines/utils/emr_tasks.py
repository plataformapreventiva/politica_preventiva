#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" EMR Utils
This module contains utilities for AWS-EMR handling
"""

import abc
import boto.emr
import boto3
import botocore
import luigi
import logging
import os
import pdb
import logging
import time
import yaml

from datetime import datetime, timedelta, timezone
from tabulate import tabulate
from luigi import configuration


# logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")


class EMRLoader(object):

    # Interval to wait between polls to EMR cluster in seconds
    CLUSTER_OPERATION_RESULTS_POLLING_SECONDS = 30
    # Timeout for EMR creation and ramp up in seconds
    CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS = 1440 * 60
    # request rate backoff duration
    CLUSTER_BACKOFF_DURATION_SECONDS = 80

    def __init__(self, aws_access_key, aws_secret_access_key, region_name,
                 cluster_name, instance_count, master_instance_type,
                 slave_instance_type, key_name, subnet_id, log_uri,
                 software_version, script_bucket_name, key, key_value):
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.cluster_name = cluster_name
        self.instance_count = instance_count
        self.master_instance_type = master_instance_type
        self.slave_instance_type = slave_instance_type
        self.key_name = key_name
        self.subnet_id = subnet_id
        self.log_uri = log_uri
        self.software_version = software_version
        self.script_bucket_name = script_bucket_name
        self.key = key
        self.key_value = key_value


    def boto_client(self, service):
        client = boto3.client(service,
                              aws_access_key_id=self.aws_access_key,
                              aws_secret_access_key=self.aws_secret_access_key,
                              region_name=self.region_name)
        return client


    def launch_cluster(self):
        job_flow_id = self.boto_client("emr").run_job_flow(
            Name=self.cluster_name,
            LogUri=self.log_uri,
            ReleaseLabel=self.software_version,
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'MasterInstanceType',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.master_instance_type,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'CoreInstanceType',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.slave_instance_type,
                        'InstanceCount': self.instance_count,
                    }
                ],

                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': self.key_name,
                'Ec2SubnetId': self.subnet_id
            },
            Applications=[
                {
                    'Name': 'Spark'
                },
                {
                    'Name': 'Ganglia'
                },
                {
                    'Name': 'Zeppelin'
                }
            ],
            Configurations=[
                {
                    "Classification": "capacity-scheduler",
                    "Properties": {
                        "yarn.scheduler.capacity.resource-calculator":
                            "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
                    }
                },
                {
                    "Classification": "spark",
                    "Properties": {
                        "maximizeResourceAllocation": "true"
                    }
                },
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.dynamicAllocation.enabled": "true",
                    }
                },
                {
                   "Classification": "spark-env",
                    "Configurations": [
                            {
                                "Classification": "export",
                                "Properties": {
                                    "PYSPARK_PYTHON": "/usr/bin/python3"
                                }
                            }
                        ]
                }
            ],
            Tags=[
                {
                    'Key': self.key,
                    'Value': self.key_value
                },
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
        self.job_flow_id = job_flow_id['JobFlowId']


    def send_pipeline_steps(self, steps, job_flow_id):
        response = self.boto_client("emr").add_job_flow_steps(JobFlowId=job_flow_id,
                                                              Steps=steps)
        return response


    def copy_steps(self, script_bucket_name, script_names):
        Steps = []
        for script_name in script_names:
            step = {'Name' : 'setup - copy {}'.format(script_name),
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', '{0}{1}'.format(script_bucket_name, script_name),
                                '/home/hadoop/{0}'.format(script_name)]
                    }
                }
            Steps.append(step)
        return Steps


    def execute_step(self, step_name, script_name, parameters=dict(), mode='spark'):
        args_parameters = [ ['--{par}'.format(par=llave), '{arg}'.format(arg=value)]
                                for llave,value in parameters.items()]
        flatten = lambda l: [item for sublist in l for item in sublist]
        flatten_args = flatten(args_parameters)
        if mode == 'spark':
            Args = ['spark-submit',
                             '--conf', 'spark.executor.instances=4',
                             '--conf', 'spark.memory.fraction=0.6',
                             '--conf', 'spark.memory.storageFraction=0.5',
                             '--conf', 'spark.yarn.executor.memoryOverhead=1024',
                             '--conf', 'spark.yarn.driver.memoryOverhead=512',
                             '--conf', 'spark.executor.memory=1g',
                             '--conf', 'spark.driver.memory=1g',
                             '--conf', 'spark.driver.cores=1',
                             '--conf', 'spark.executor.cores=1',
                             '--conf', 'spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3',
                             '--conf', 'spark.executorEnv.PYSPARK_PYTHON=python3',
                             '/home/hadoop/{}'.format(script_name)] +  flatten_args

        elif mode == 'python':
            Args = ['python3', '/home/hadoop/{}'.format(script_name)] +  flatten_args

        Step =  [{'Name': step_name,
                  'ActionOnFailure': 'CANCEL_AND_WAIT',
                  'HadoopJarStep': {
                  'Jar': 'command-runner.jar',
                  'Args': Args
                }
             }]
        return Step


    def general_steps(self):
        # sudo python3 -m pip install --upgrade pip
        step_py = [{
                    'Name': 'install python packages',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo','sh',
                                 '/home/hadoop/set_up.sh']}
                }]
        return step_py


    def shutdown_emr_cluster(self, job_flow_id, cluster_path):
        if os.path.exists(cluster_path):
            os.remove(cluster_path)

        try:
            conn = boto.emr.connect_to_region('us-west-2')
            conn.terminate_jobflow(job_flow_id)
        except:
            logger.critical("Cluster shutted down unexpectedly before Luigi's attempt.")


    def get_job_flow_id(self):
        """
        Get the id of the clusters WAITING for work
        """
        self.boto_client("emr").list_clusters(cluster_states=['WAITING']).clusters[0].id


    def poll_until_all_jobs_completed(self, job_flow_id, step_name):
        start_time = time.time()
        is_job_finished = False
        completed = False
        print_execution_summary_flag = 0

        # polling
        while (not is_job_finished) and (
            time.time() - start_time < EMRLoader.CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS):
            try:
                lista = self.boto_client("emr").list_steps(ClusterId=job_flow_id)
                lista_name = [x for x in lista['Steps'] if x['Name'] == step_name][0]
                state = lista_name['Status']['State']
                logger.info("Running step: {step}".format(step=step_name))
                logger.info("Step state: {state}".format(state=state))
                if (state == u'TERMINATED') or (state == u'COMPLETED'):
                    is_job_finished = True
                    completed = True
                    return completed
                elif (state == u'FAILED') or (state == u'CANCELLED'):
                    logger.error('Step stopped running with FAILED or CANCELLED status.')
                    is_job_finished = True
                print_execution_summary_flag += 1
                time.sleep(EMRLoader.CLUSTER_OPERATION_RESULTS_POLLING_SECONDS)
                if print_execution_summary_flag % 5 == 0:
                    now = datetime.now(timezone.utc)
                    lista_sum = [ {'Name':l['Name'],
                                    'Status':l['Status']['State'],
                                    'Creation time':(l['Status']['Timeline']['CreationDateTime']-timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")} \
                            for l in lista['Steps'] ]
                    nombres = {x:x for x in lista_sum[0].keys()}
                    sum_print = '\n\n ' + tabulate(lista_sum, nombres) + '\n'
                    logger.info(sum_print)
                    logger.info("WAITING for further response from EMR Cluster.")
            except:
                logger.debug("""AWS: (ThrottlingException) Maximum send rate exceeded when calling
                    the ListSteps operation (reached max retries: 4): Rate exceeded""")
                try:
                    logger.info("WAITING for further response from EMR Cluster.")
                    time.sleep(EMRLoader.CLUSTER_BACKOFF_DURATION_SECONDS)
                except:
                    logger.critical("Waiting exception.")

        return completed


    def get_final_status(self, job_flow_id, step_completed, cluster_path, steps_pending=True):

        job_flow_status = ""
        if (step_completed == True) and (steps_pending == True):
            # Keep cluster alive
            cluster_description = self.boto_client("emr").describe_cluster(ClusterId=job_flow_id)
            status = cluster_description['Cluster']['Status']['State']
            logger.info("EMR cluster state: {status}".format(status=status))
            job_flow_status = "Pending steps"
        elif (step_completed == True) and (steps_pending == False):
            # Terminate EMR cluster
            cluster_description = self.boto_client("emr").describe_cluster(ClusterId=job_flow_id)
            status = cluster_description['Cluster']['Status']['State']
            logger.info("All steps completed successfully.")
            logger.info('Attempting to shut down the cluster with current status: %s' % status)
            job_flow_status = "Completed"
            self.shutdown_emr_cluster(job_flow_id=job_flow_id, cluster_path=cluster_path)
        elif (step_completed == False) and (steps_pending == True):
            logger.critical("STEP FAILED WITH PENDING STEPS.")
            job_flow_status = "Step failed with pending steps"
            self.shutdown_emr_cluster(job_flow_id=job_flow_id, cluster_path=cluster_path)
        elif (step_completed == False) and (steps_pending == False):
            job_flow_status = "Failed with no pending steps."
            self.shutdown_emr_cluster(job_flow_id=job_flow_id, cluster_path=cluster_path)

        return True

