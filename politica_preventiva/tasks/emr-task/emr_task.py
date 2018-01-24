

class EmrTask(luigi.Task):

    """
    Abstract EMR Luigi Task
    """
    # Load EMR Configuratio
    emr_loader = emr_tasks.EMRLoader(
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
        key_value=config_emr.get("key_value"))
    emr_client = emr_loader.boto_client("emr")

class AddStep(EmrTask):
    id_name = luigi.Parameter()

    def requires(self):
        return InitializeCluster(self.id_name)


class InitializeCluster(EmrTask):
    """
    Luigi Task to initialize  EMR cluster.
    This Task writes an output token to the location designated by the
    created. The Task will fail if the cluster cannot be initialized.
    """

    common_path = luigi.Parameter('DEFAULT')
    emr_path = luigi.Parameter('DEFAULT')
    spark_bucket = luigi.Parameter('DEFAULT')
    id_name = luigi.Parameter()

    def run(self):
        """
        Create the EMR cluster
        """
        cluster_id = self.common_path + "emr_id_" + self.id_name + ".txt"
        # Launch Cluster
        self.emr_loader.launch_cluster()
        # Create Client
        self.emr_loader.boto_client("emr")
        file = open(cluster_id,'w')
        file.write(self.emr_loader.job_flow_id)
        file.close()
        # Put to S3
        requirements_file = self.emr_path + 'requirements.txt'
        requirements_output = self.spark_bucket + 'requirements.txt'
        self.client.put(requirements_file, requirements_output)
        copy_steps = self.copy_steps(self.spark_bucket, ['requirements.txt'])

        steps = copy_steps + self.general_steps()
        self.send_pipeline_steps(steps, self.emr_loader.job_flow_id)

    def output(self):
        cluster_id = self.common_path + "emr_id_" + self.id_name + ".txt"
        return luigi.LocalTarget(cluster_id)
