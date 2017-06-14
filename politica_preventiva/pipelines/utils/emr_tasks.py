#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" EMR Utils

This module contains utilities for Luigi EMR Task
Taken from https://gist.github.com/zachradtka/e905bc70db1a42933aba

"""
import abc
import time
 
import boto
from boto.emr.connection import EmrConnection
from boto.regioninfo import RegionInfo
from boto.emr.step import InstallPigStep
 
import luigi
from luigi.s3 import S3Target, S3PathTask
 
from djcity.luigi import target_factory


import logging
 
logger = logging.getLogger('luigi-interface')
 
 
def create_task_s3_target(root_path, run_path, task_str):
    """
    Helper method for easily creating S3Targets
    """
    return S3Target('%s/%s/%s' % (root_path, run_path, task_str))
 
 
class EmrClient(object):


    # The Hadoop version to use
    HADOOP_VERSION = '1.0.3'

    # The AMI version to use
    AMI_VERSION = '2.4.7'
 
    # Interval to wait between polls to EMR cluster in seconds
    CLUSTER_OPERATION_RESULTS_POLLING_SECONDS = 10
 
    # Timeout for EMR creation and ramp up in seconds
    CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS = 60 * 30
 
    def __init__(self, region_name='us-east-1', aws_access_key_id=None, aws_secret_access_key=None):
 
        # If the access key is not specified, get it from the luigi config.cfg file
        if not aws_access_key_id:
            aws_access_key_id = luigi.configuration.get_config().get('aws', 'aws_access_key_id')
 
        if not aws_secret_access_key:
            aws_secret_access_key = luigi.configuration.get_config().get('aws', 'aws_secret_access_key')
 
 
        # Create the region in which to run
        region_endpoint = u'elasticmapreduce.%s.amazonaws.com' % (region_name)
        region = RegionInfo(name=region_name, endpoint=region_endpoint)
 
        self.emr_connection = EmrConnection(aws_access_key_id=aws_access_key_id,
                                            aws_secret_access_key=aws_secret_access_key,
                                            region=region)
 
    def launch_emr_cluster(self, cluster_name, log_uri, ec2_keyname=None, master_type='m1.small', core_type='m1.small', num_instances=2, hadoop_version='1.0.3', ami_version='2.4.7', ):
 
        # Install Pig
        install_pig_step = InstallPigStep()
 
        jobflow_id = self.emr_connection.run_jobflow(name=cluster_name,
                              log_uri=log_uri,
                              ec2_keyname=ec2_keyname,
                              master_instance_type=master_type,
                              slave_instance_type=core_type,
                              num_instances=num_instances,
                              keep_alive=True,
                              enable_debugging=True,
                              hadoop_version=EmrClient.HADOOP_VERSION,
                              steps=[install_pig_step], 
                              ami_version=EmrClient.AMI_VERSION)
 
        # Log important information
        logger.info('Creating new cluster %s with following details' % \
            self.emr_connection.describe_jobflow(jobflow_id).name)
        logger.info('jobflow ID:\t%s' \
            % self.emr_connection.describe_jobflow(jobflow_id).jobflowid)
        logger.info('Log URI:\t%s' \
            % self.emr_connection.describe_jobflow(jobflow_id).loguri)
        logger.info('Master Instance Type:\t%s' \
            % self.emr_connection.describe_jobflow(jobflow_id).masterinstancetype)
        logger.info('Slave Instance Type:\t%s' \
            % self.emr_connection.describe_jobflow(jobflow_id).slaveinstancetype)
        logger.info('Number of Instances:\t%s' \
            % self.emr_connection.describe_jobflow(jobflow_id).instancecount)
        logger.info('Hadoop Version:\t%s' \
            % self.emr_connection.describe_jobflow(jobflow_id).hadoopversion)
        logger.info('AMI Version:\t%s' \
            % self.emr_connection.describe_jobflow(jobflow_id).amiversion)
        logger.info('Keep Alive:\t%s' \
            % self.emr_connection.describe_jobflow(jobflow_id).keepjobflowalivewhennosteps)
 
        return self._poll_until_cluster_ready(jobflow_id)
 
 
    def shutdown_emr_cluster(self, jobflow_id):
 
        self.emr_connection.terminate_jobflow(jobflow_id)
        return self._poll_until_cluster_shutdown(jobflow_id)
 
    def get_jobflow_id(self):
        # Get the id of the cluster that is WAITING for work
        return self.emr_connection.list_clusters(cluster_states=['WAITING']).clusters[0].id
 
    def _poll_until_cluster_ready(self, jobflow_id):
 
        start_time = time.time()
 
        is_cluster_ready = False
 
        while (not is_cluster_ready) and (time.time() - start_time < EmrClient.CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS):
            # Get the state
            state = self.emr_connection.describe_jobflow(jobflow_id).state

            if state == u'WAITING':
                logger.info('Cluster intialized and is WAITING for work')
                is_cluster_ready = True

            elif (state == u'COMPLETED') or \
                 (state == u'SHUTTING_DOWN') or \
                 (state == u'FAILED') or \
                 (state == u'TERMINATED'):
                
                logger.error('Error starting cluster; status: %s' % state)

                # Poll until cluster shutdown
                self._poll_until_cluster_shutdown(jobflow_id)
                raise RuntimeError('Error, cluster failed to start')

            else:
                logger.debug('Cluster state: %s' % state)
                time.sleep(EmrClient.CLUSTER_OPERATION_RESULTS_POLLING_SECONDS)

 
        if not is_cluster_ready:
            # TODO shutdown cluster
            raise RuntimeError('Timed out waiting for EMR cluster to be active')
 
        return jobflow_id
 
 
    def _poll_until_cluster_shutdown(self, jobflow_id):
        start_time = time.time()
 
        is_cluster_shutdown = False
 
        while (not is_cluster_shutdown) and (time.time() - start_time < EmrClient.CLUSTER_OPERATION_RESULTS_TIMEOUT_SECONDS):
            # Get the state
            state = self.emr_connection.describe_jobflow(jobflow_id).state

            if (state == u'TERMINATED') or (state == u'COMPLETED'):
                logger.info('Cluster successfully shutdown with status: %s' % state)
                return False
            elif state == u'FAILED':
                logger.error('Cluster shutdown with FAILED status')
                return False
            else:
                logger.debug('Cluster state: %s' % state)
                time.sleep(EmrClient.CLUSTER_OPERATION_RESULTS_POLLING_SECONDS)

        if not is_cluster_shutdown:
            # TODO shutdown cluster
            raise RuntimeError('Timed out waiting for EMR cluster to shut down')
 
        return True
 
 
class EmrTask(luigi.Task):
 
    @abc.abstractmethod
    def output_token(self):
        """
        Luigi Target providing path to a token that indicates completion of this Task.
        :rtype: Target:
        :returns: Target for Task completion token
        """
        raise RuntimeError("Please implement the output_token method")
 
    def output(self):
        """
        The output for this Task. Returns the output token by default, so the task only runs if the 
        token does not already exist.
        :rtype: Target:
        :returns: Target for Task completion token
        """
        return self.output_token()
 
 
class InitializeEmrCluster(EmrTask):
    """
    Luigi Task to initialize a new EMR cluster.
    This Task writes an output token to the location designated by the `output_token` method to 
    indicate that the clustger has been successfully create. The Task will fail if the cluster
    cannot be initialized.
    Cluster creation in EMR takes between several seconds and several minutes; this Task will
    block until creation has finished.
    """
 
    cluster_name = luigi.Parameter(default='EMR Cluster')
    ec2_keyname = luigi.Parameter()
    log_uri = luigi.Parameter()
 
    def run(self):
        """
        Create the EMR cluster
        """
 
        emr_client = EmrClient()
        emr_client.launch_emr_cluster(cluster_name=self.cluster_name, 
                                      log_uri=self.log_uri, 
                                      ec2_keyname=self.ec2_keyname)
 
        target_factory.write_file(self.output_token())


class TerminateEmrCluster(EmrTask):
    
    def run(self):

        emr_client = EmrClient()
        jobflow_id = emr_client.get_jobflow_id()

        emr_client.shutdown_emr_cluster(jobflow_id)

        target_factory.write_file(self.output_token())



class ExampleCreateEmrCluster(InitializeEmrCluster):
    """
    This task creates an EMR cluster for runnig the rest of the tasks on
    """
 
    output_root_path = luigi.Parameter()
    output_run_path = luigi.Parameter()
 
    def output_token(self):
        return create_task_s3_target(self.output_root_path, 
                                     self.output_run_path, 
                                     self.__class__.__name__)


class ExampleShutdownEmrCluster(TerminateEmrCluster):
    """
    This task terminates an EMR cluster
    """
 
    output_root_path = luigi.Parameter()
    output_run_path = luigi.Parameter()
 
    def output_token(self):
        return create_task_s3_target(self.output_root_path, 
                                     self.output_run_path, 
                                     self.__class__.__name__)
