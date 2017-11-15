# coding: utf-8
import luigi
import logging
import os
import pdb
import subprocess

from dotenv import load_dotenv, find_dotenv
from itertools import product
from luigi import configuration
import pandas as pd

from politica_preventiva.pipelines.ingest.classic.classic_ingest_tasks import *
from politica_preventiva.pipelines.ingest.classic.preprocessing_scripts.preprocessing_scripts import *
from politica_preventiva.pipelines.ingest.tools.ingest_utils import\
        parse_cfg_list, get_extra_str, s3_to_pandas, find_extension
from politica_preventiva.pipelines.utils.pg_sedesol import parse_cfg_string

conf = configuration.get_config()

# AWS
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
PLACES_API_KEY = os.environ.get('PLACES_API_KEY')

# Logger
logging_conf = configuration.get_config().get("core", "logging_conf_file")
logging.config.fileConfig(logging_conf)
logger = logging.getLogger("dpa-sedesol")

#######################
# Abstract Tasks
#########


class SourceIngestTask(luigi.Task):

    data_date = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()
    classic_task_scripts = luigi.Parameter('DEFAULT')
    local_path = luigi.Parameter('DEFAULT')
    extra = luigi.Parameter()

    def requires(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        logger.info('Luigi is trying to run the source script' +
                    ' of the pipeline_task {0}'.format(self.pipeline_task))

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)


class TDockerTask(SourceIngestTask):

    """
    Task Abstraction to Dockerize source ingest tasks
    it uses the image python-task
       see more in pipeline/task/python-task/Dockerfile

    Note:
    - politica_preventiva_storeshould already exists
    - Remember to use local_ingest_file instead of local_ingest_file
    - It can run python and bash scripts.

    Use:
    Define the @property def cmd(self):
        python **/**.py
        bash **/**.sh

    See ipc_ciudades or cuenta_publica_anual for reference
    # TODO() This task will eventually replace SourceIngestTask
    # Check if pdb can be used for debugin purposes
    """

    def run(self):

        logger.info('Luigi is using the dockerized version of the task' +
                    ' {0}'.format(self.pipeline_task))

        cmd_docker = '''
         docker run -it --rm  -v $PWD:/politica_preventiva\
                -v politica_preventiva_store:/data\
           politica_preventiva/task/docker-task {0} > /dev/null
         '''.format(self.cmd)
        out = subprocess.call(cmd_docker, shell=True)
        logger.info(out)

#######################
# Classic Ingest Tasks
#######


class denue(TDockerTask):

    @property
    def cmd(self):

        command_list = ['sh', self.classic_task_scripts +
                        'denue.sh', self.local_path +
                        self.pipeline_task, self.local_ingest_file]

        return " ".join(command_list)


class cuenta_publica_trimestral(TDockerTask):
    @property
    def cmd(self):

        command_list = ['sh', self.classic_task_scripts +
                        'cuenta_publica_trimestral.sh', self.data_date,
                        self.local_path +
                        self.pipeline_task, self.local_ingest_file]

        return " ".join(command_list)


class cuenta_publica_anual(TDockerTask):

    @property
    def cmd(self):
        # year = self.data_date.split("-")[0]
        command_list = ['sh', self.classic_task_scripts +
                        'cuenta_publica_anual.sh', self.data_date,
                        self.local_path +
                        self.pipeline_task, self.local_ingest_file]

        return " ".join(command_list)


class sagarpa(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        # extra_cmd = self.extra.split('--')
        # cultivo = extra_cmd[0]

        command_list = ['python', self.classic_task_scripts + "sagarpa.py",
                        '--start', self.data_date, '--cult', self.extra,
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class sagarpa_cierre(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)
        # extra_cmd = self.extra.split('--')
        # estado = self.extra_cmd[0]

        command_list = ['python', self.classic_task_scripts + "sagarpa.py",
                        '--start', self.data_date, '--estado', self.extra,
                        '--cierre', 'True', '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class ipc_ciudades(SourceIngestTask):

    @property
    def cmd(self):
        cmd = '''
            python {0}ipc.py\
            --year {1} --output {2}
        '''.format(self.classic_task_scripts, self.data_date,
                   self.local_ingest_file)
        return cmd


class segob_snim(TDockerTask):

    @property
    def cmd(self):
        extra_cmd = self.extra.split('--')
        extra_cmd = extra_cmd[0]
        command_list = ['python', self.classic_task_scripts +
                        "segob_snim.py", '--data_date', self.data_date,
                        '--output', self.local_ingest_file, "--extra",
                        extra_cmd]
        return " ".join(command_list)


class precios_granos(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        extra_cmd = self.extra.split('--')
        end_date = extra_cmd[0]
        if end_date:
            end_cmd = " ".join(['--end', end_date])
        else:
            end_cmd = ""

        command_list = ['python', self.classic_task_scripts + "economia.py",
                        '--start', self.data_date, end_cmd,
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class precios_frutos(SourceIngestTask):

    def run(self):

        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        extra_cmd = self.extra.split('--')
        mercado = extra_cmd[0]

        command_list = ['python', self.classic_task_scripts +
                        "economia_frutos.py", '--start', self.data_date,
                        '--mercado', mercado, '--output',
                        self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class distance_to_services(luigi.Task):

    """

    Task que descarga la distancia a servicios básicos de la base de
    Google.
    TODO(Definir keywords dinamicamente)

    """

    client = luigi.s3.S3Client()
    data_date = luigi.Parameter()
    pipeline_task = luigi.Parameter()
    local_ingest_file = luigi.Parameter()

    classic_task_scripts = luigi.Parameter('ClassicIngest')
    local_path = luigi.Parameter('DEFAULT')
    raw_bucket = luigi.Parameter('DEFAULT')

    extra = luigi.Parameter()

    def run(self):

        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("""SELECT cve_muni, latitud, \
                longitud FROM geoms.municipios""")
        rows = pd.DataFrame(cur.fetchall(),
                            columns=["cve_muni", "lat", "long"])
        rows = rows[:5]

        # ["Hospital","doctor","bus_station","airport","bank", \
        # "gas_station","university","subway_station","police"]
        for keyword in ["Hospital", "bank", "university", "police"]:
            print("looking for nearest {0}".format(keyword))
            vector_dic = rows.apply(lambda x: 
                                    info_to_google_services(x["lat"],
                                                            x["long"],
                                                            keyword),
                                    axis=1)
            rows[['driving_dist_{0}'.format(keyword),
                  'driving_time_{0}'.format(keyword),
                  'formatted_address_{0}'.format(keyword),
                  'local_phone_number_{0}'.format(keyword),
                  'name_{0}'.format(keyword),
                  'walking_dist_{0}'.format(keyword),
                  'walking_time_{0}'.format(keyword),
                  'website_{0}'.format(keyword)]] = pd.DataFrame(list(vector_dic))

        return rows.to_csv(self.output().path, index=False, sep="|")

    def output(self):
        return luigi.LocalTarget(self.local_ingest_file)


class cenapred(SourceIngestTask):
    """
    Task que descarga los datos de cenapred
    Ver classic_task_scripts.cenapred.py para más información
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.classic_task_scripts + "cenapred.py",
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class cajeros_banxico(SourceIngestTask):
    """
    Task que descarga los cajeros actualizados de la base de datos Banxico
    Ver classic_task_scripts.cajeros_banxico.py para más información
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.classic_task_scripts +
                        "cajeros_banxico.py",
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)

        return subprocess.call([cmd], shell=True)


class indesol(SourceIngestTask):
    """
    Task que descarga las ong's con clave CLUNI de INDESOL
    Ver classic_task_scripts.indesol.sh para más información
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts + "indesol.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]

        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class donatarias_sat(SourceIngestTask):
    """
    Task que descarga las donatarias autorizadas por SAT cada año
    Ver bash_cripts.
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)
        command_list = ['sh', self.classic_task_scripts + "donatarias_sat.sh",
                        self.data_date,
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]

        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class cuaps_sedesol(SourceIngestTask):
    """
    Task que descarga el diccionario de programas CUAPS 
    desarrollado por SEDESOL
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['python', self.classic_task_scripts +
                        "cuaps_sedesol.py",
                        '--start', self.data_date,
                        '--output', self.local_ingest_file]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)


class mir(SourceIngestTask):
    """
    Task que descarga la matriz de indicadores para resultados
    """
    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts + "mir.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]

        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class msd(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts + "msd.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class evals(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts + "evals.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class asm(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts + "asm.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class finanzas_publicas_estatales(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts +
                        "finanzas_publicas_estatales.sh",
                        self.data_date, self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class finanzas_publicas_municipales(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts +
                        "finanzas_publicas_municipales.sh",
                        self.data_date, self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class iter_2010(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts + "iter_2010.sh",
                        self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class defunciones_generales(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts +
                        "defunciones_generales.sh",
                        self.data_date, self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class defunciones_fetales(SourceIngestTask):

    def run(self):
        if not os.path.exists(self.local_path + self.pipeline_task):
            os.makedirs(self.local_path + self.pipeline_task)

        command_list = ['sh', self.classic_task_scripts +
                        "defunciones_fetales.sh",
                        self.data_date, self.local_path + self.pipeline_task,
                        self.local_ingest_file]
        cmd = " ".join(command_list)

        return subprocess.call([cmd], shell=True)


class coneval_estados(TDockerTask):
    @property
    def cmd(self):

        command_list = ['sh', self.classic_task_scripts +
                        'coneval_estados.sh', self.data_date,
                        self.local_path +
                        self.pipeline_task, self.local_ingest_file]

        return " ".join(command_list)


class coneval_municipios(TDockerTask):
    @property
    def cmd(self):
        command_list = ['python', self.classic_task_scripts +
                        'coneval_municipios.py',
                        '--data_date', self.data_date,
                        '--local_path', self.local_path + self.pipeline_task,
                        '--local_ingest_file', self.local_ingest_file]
        return " ".join(command_list)


class declaratoria(TDockerTask):
    @property
    def cmd(self):

        command_list = ['sh', self.classic_task_scripts +
                        'declaratoria.sh', self.data_date,
                        self.local_path +
                        self.pipeline_task, self.local_ingest_file]

        return " ".join(command_list)

class insp(TDockerTask):

    @property
    def cmd(self):
        year = self.data_date.split("-")[0]
        month = self.data_date.split("-")[1].zfill(2)
        command_list = ['sh', self.classic_task_scripts +
                        'insp.sh', year, month,
                        self.local_path +
                        self.pipeline_task, self.local_ingest_file]

        return " ".join(command_list)


class general_ingest(TDockerTask):
    """
    This general ingest tasks looks for a script in 
    classic_task_scripts with the pipeline task name.
    """
    @property
    def cmd(self):
        extension = find_extension(self.classic_task_scripts,
                                   self.pipeline_task + '.')
        command_list = [extension[0],
                        self.classic_task_scripts +
                        self.pipeline_task + '.' +
                        extension[1],
                        self.data_date,
                        self.local_path +
                        self.pipeline_task,
                        self.local_ingest_file]

class comedores(TDockerTask):
    @property
    def cmd(self):
        # year = self.data_date.split("-")[0]
        # month = self.data_date.split("-")[1].zfill(2)
        command_list = ['sh', self.classic_task_scripts +
                        'comedores.sh', self.data_date,
                        self.local_path +
                        self.pipeline_task, self.local_ingest_file]
        return " ".join(command_list)


class estancias(TDockerTask):
    @property
    def cmd(self):
        # year = self.data_date.split("-")[0]
        # month = self.data_date.split("-")[1].zfill(2)
        command_list = ['sh', self.classic_task_scripts +
                        'estancias.sh', self.data_date,
                        self.local_path +
                        self.pipeline_task, self.local_ingest_file]
        return " ".join(command_list)

class lecherias(TDockerTask):
    @property
    def cmd(self):
        # year = self.data_date.split("-")[0]
        # month = self.data_date.split("-")[1].zfill(2)
        command_list = ['sh', self.classic_task_scripts +
                        'lecherias.sh', self.data_date,
                        self.local_path +
                        self.pipeline_task, self.local_ingest_file]
        return " ".join(command_list)
