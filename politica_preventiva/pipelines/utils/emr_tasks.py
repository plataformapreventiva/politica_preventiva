#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" EMR Utils
This module contains utilities for AWS-EMR handling
"""

import abc
import boto3
import botocore
import luigi
import logging
import pdb
import logging
import time
import yaml

from luigi import configuration


# logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")


class EMRLoader(object):

    # Interval to wait between polls to EMR cluster in seconds
    CLUSTER_OPERATION_RESULTS_POLLING_SECONDS = 10
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
                    #,
                    #{
                    #    'Name': 'TaskInstanceType',
                    #    'Market': 'SPOT',
                    #    'InstanceRole': 'TASK',
                    #    'BidPrice': '0.001',  # self.bidprice
                    #    'InstanceType': self.slave_instance_type,
                    #    'InstanceCount': 1,
                    #}
                ],

                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
                'Ec2KeyName': self.key_name,
                'Ec2SubnetId': self.subnet_id
            },
            Applications=[
                {
                    'Name': 'Spark'
                    # 'Version':'2.1.0'
                },
                {
                    'Name': 'Ganglia'
                    # 'Version': '3.7.2'
                },
                {
                    'Name': 'Zeppelin'
                    # 'Version':'0.7.0'
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
        return True  # self._poll_until_cluster_ready(job_flow_id)


    def add_pipeline_step(self, job_flow_id, step_name, script_bucket_name,
                          script_name, parameter='', value=''):
        response = self.boto_client("emr").add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=[
                {
                    'Name': 'setup - copy files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', 's3://{0}/{1}'.format(script_bucket_name, script_name),
                                 '/home/hadoop/']
                    }
                },
                {
                    'Name': 'install python packages',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo','pip','install','boto3','pandas']
                    }
                },
                #{
                #    'Name': 'install python packages',
                #    'ActionOnFailure': 'CANCEL_AND_WAIT',
                #    'HadoopJarStep': {
                #        'Jar': 'command-runner.jar',
                #        'Args': ['sudo','pip','install','boto3',
                #                '/home/hadoop']
                #    }
                #},
                {
                    'Name': step_name,
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', 
                                 '--conf', 'spark.executor.instances=6', 
                                 '--conf', 'spark.memory.fraction=0.8',
                                 '--conf', 'spark.memory.storageFraction=0.3',
                                 '--conf', 'spark.yarn.executor.memoryOverhead=1024',
                                 '--conf', 'spark.yarn.driver.memoryOverhead=512',
                                 '--conf', 'spark.executor.memory=9g',
                                 '--conf', 'spark.driver.memory=2g',
                                 '--conf', 'spark.driver.cores=1',
                                 '--conf', 'spark.executor.cores=7',
                                 '--conf', 'spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3',
                                 '--conf', 'spark.executorEnv.PYSPARK_PYTHON=python3',
                                 '/home/hadoop/' + script_name, 
                                 parameter, value]
                    }
                }
            ])

    def shutdown_emr_cluster(self, job_flow_id):
        self.boto_client("emr").terminate_job_flow(job_flow_id)
        return self._poll_until_cluster_shutdown(job_flow_id)

    def get_job_flow_id(self):
        """
        Get the id of the clusters WAITING for work
        """
        self.boto_client("emr").list_clusters(cluster_states=['WAITING']).clusters[0].id


    def _poll_until_cluster_ready(self, job_flow_id):
        start_time = time.time()
        is_cluster_ready = False
        while (not is_cluster_ready) and (
                time.time() - start_time < EMRLoader.CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS):
            # Get the state
            state = self.boto_client("emr").describe_job_flows(
                JobFlowIds=[self.job_flow_id]).state
            if state == u'WAITING':
                logger.info('Cluster intialized and is WAITING for work')
                is_cluster_ready = True
            elif (state == u'COMPLETED') or \
                    (state == u'SHUTTING_DOWN') or \
                    (state == u'FAILED') or \
                    (state == u'TERMINATED'):
                logger.error('Error starting cluster; status: %s' % state)
                # Poll until cluster shutdown
                self._poll_until_cluster_shutdown(job_flow_id)
                raise RuntimeError('Error, cluster failed to start')
            else:
                logger.debug('Cluster state: %s' % state)
                time.sleep(EMRLoader.CLUSTER_OPERATION_RESULTS_POLLING_SECONDS)
        if not is_cluster_ready:
            #TODO shutdown cluster
            raise RuntimeError('Timed out waiting for EMR cluster to be active')
        return job_flow_id


    def get_final_status(self, job_flow_id, Name):
        start_time = time.time()
        is_job_finished = False

        while (not is_job_finished) and (
            time.time() - start_time < EMRLoader.CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS):

            try:
                lista = self.boto_client("emr").list_steps(ClusterId=job_flow_id)
                lista_name = [x for x in lista['Steps'] if x['Name'] == Name][-1]
                state = lista_name['Status']['State']
                print(state)
                if (state == u'TERMINATED') or (state == u'COMPLETED'):
                    is_job_finished = True
                    return True
                elif (state == u'FAILED') or (state == u'CANCELLED'):
                    logger.error('Step stopped running with FAILED or CANCELLED status.')
                    is_job_finished = True
            except:
                logger.debug("""AWS: (ThrottlingException) Maximum send rate exceeded when calling
                    the ListSteps operation (reached max retries: 4): Rate exceeded""")
                try:
                    print("WAITING")
                    time.sleep(EMRLoader.CLUSTER_BACKOFF_DURATION_SECONDS)
                except:
                    print("Waiting exception.")

        return False


    def _poll_until_cluster_shutdown(self, job_flow_id):
        start_time = time.time()
        is_cluster_shutdown = False
        while (not is_cluster_shutdown) and (
                time.time() - start_time < EMRLoader.CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS):
            # Get the state
            state = self.boto_client("emr").describe_job_flows(JobFlowIds=[job_flow_id["JobFlowId"]]).state
            if (state == u'TERMINATED') or (state == u'COMPLETED'):
                logger.info('Cluster successfully shutdown with status: %s' % state)
                return False
            elif state == u'FAILED':
                logger.error('Cluster shutdown with FAILED status')
                return False
            else:
                logger.debug('Cluster state: %s' % state)
                time.sleep(EMRLoader.CLUSTER_OPERATION_RESULTS_POLLING_SECONDS)
        if not is_cluster_shutdown:
            raise RuntimeError('Timed out waiting for EMR cluster to shut down')
        return True

