#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" EMR Utils

This module contains utilities for AWS-EMR handling

"""

import boto3
import botocore
import yaml
import time
import logging

class EMRLoader(object):
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

    def load_cluster(self):

        response = self.boto_client("emr").run_job_flow(
            
            Name=self.cluster_name,
            LogUri=self.log_uri,
            ReleaseLabel=self.software_version,
            
            Instances={
                'InstanceGroups' : [
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
		 },
		 {
	  	    'Name': 'TaskInstanceType',
		    'Market': 'SPOT',
		    'InstanceRole': 'TASK',
		    'BidPrice': '0.01', # self.bidprice
		    'InstanceType': self.slave_instance_type,
		    'InstanceCount': 2,
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
			    "spark.executor.instances": "0"
			    }
			} 
	    ],

            Tags = [
                {
                    'Key': self.key,
                    'Value': self.key_value

                },
                
            ],
            
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
        #logger.info(response)
        
        return response

    def add_step(self, job_flow_id, master_dns):
        response = self.boto_client("emr").add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=[
                {
                    'Name': 'setup - copy files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                     'Jar': 'command-runner.jar',
                     'Args': ['aws', 's3', 'cp',
                     's3://{script_bucket_name}/pyspark_quick_setup.sh'.format(
                               script_bucket_name=self.script_bucket_name),
                                 '/home/hadoop/']
                    }
                },
                {
                    'Name': 'setup pyspark with conda',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'bash', 
			'/home/hadoop/pyspark_quick_setup.sh', master_dns]
                    }
                }
            ]
        )
        #logger.info(response)
        return response
