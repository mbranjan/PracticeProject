from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.utils.email import send_email
from bfdms.dpaas import BFDMSDataprocCreateClusterOperator
from bfdms.dpaas import BFDMSDataprocSubmitJobOperator as DataprocSubmitJobOperator
from bfdms.dpaas import BFDMSDataprocDeleteClusterOperator as DataprocDeleteClusterOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.bash_operator import BashOperator
import base64

sshHook = SSHHook('prod17-ssh-conn')

SERVICE_ACCOUNT = Variable.get("service_account")
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]

SUCCESS_MSG = "repl_sku_dc_shave_dly table load was successful"

CLUSTER_NAME = "repl-dl-ephe-repl-sku-dc-shave"
REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_sku_dc_shave_dly_dag"
DAG_DESC = "repl_sku_dc_shave_dly table load job"
APP_NAME = "repl_sku_dc_shave_dly"

dpaas_config = {
    'bfd_config': {
        'dpaas_ver': '2.1',
        'lifecycle': 'prod',  # update lifecycle to build either prod or nonprod cluster
        'team': {
            'tr_product_id': '886',
        },
    },
    "cluster_config": {
        "gce_cluster_config": {
            "service_account": SERVICE_ACCOUNT,
            "metadata": {
                "enable-pepperdata": "true",  # Default
                "ha_flag": "no"  # Now the default for 2.1+
            }
        },
        "master_config": {
            "machine_type_uri": "e2-standard-8",
            "disk_config": {
                "boot_disk_size_gb": 200
            }
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 200
            }
        },
        "secondary_worker_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 200
            }
        },
        'lifecycle_config': {
            'auto_delete_ttl': {'seconds': 21600},
            'idle_delete_ttl': {'seconds': 900}
        },
        "software_config": {
            "image_version": "2.1",
            "properties": {
                "dataproc:dataproc.cluster-ttl.consider-yarn-activity": "false"
            }
        }
    }
}


def success_email_function(context):
    dag_run = context.get('dag_run')
    msg = SUCCESS_MSG
    subject = f"DAG Success Alert :  {dag_run} "
    send_email(to=EMAIL_ID, subject=subject, html_content=msg)


default_args = {
    'depends_on_past': False,
    'catchup': False,
    'email': [EMAIL_ID],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

repl_sku_dc_shave_dly_load_dag = DAG(
    DAG_NAME,
    description=DAG_DESC,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    max_active_runs=1,
    concurrency=5,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['ReplFullLoadModule']
)

PARENT_PROJECT = "wmt-dca-repl-dl-prod"
PROJECT = "wmt-dca-repl-dl-prod"
DATASET = "ei_inventory_metrics"
MATERIALIZATION_DATASET = "US_REPL_BQ_STG"

LANDING_GCS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/raw/incremental/us/ww_repl_dl_secure/repl_sku_dc_shave/"
LndngPth = LANDING_GCS_PATH + "grs"
RUN_MODE = "global"
RUN_DATE = str(date.today())
GEO_RGN_CD = "US"
BASEDIVNBR = "1"
OP_CMPNY_CD = "WMT-US"
TGT_TBL = Variable.get("repl_sku_dc_shave_dly_load_dag_var", deserialize_json=True)["tgt_tbl"]
NUM_PARTNS = "5"

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME,
    dag=repl_sku_dc_shave_dly_load_dag,
    dpaas_config=dpaas_config,
    trigger_rule='all_success',
    gcp_conn_id=CONN_ID)


def create_spark_job(WORKFLOW_NAME):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Full_Load_Module/ReplFullLoadModule-1.107.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.8-bundled.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar",
                              REPL_JAR_PATH + "hive-serde-ebcdic2ascii-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkflowController",
            "properties": {
                "spark.driver.memory": "10g",
                "spark.executor.memory": "8g",
                "spark.executor.cores": "4",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.executor.instances": "10",
                "spark.submit.deployMode": "cluster",
                "spark.locality.wait": "0",
                "spark.dynamicAllocation.enabled": "false",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.shuffle.partitions": "400",
                "spark.yarn.executor.memoryOverhead": "1g",
                "spark.yarn.driver.memoryOverhead": "1g",
                "spark.driver.extraJavaOptions": "-Dudp.conn.file=connection.yaml -Dudp.job.file=archetype_spark.yaml",
                "spark.sql.legacy.charVarcharAsString": "true",
                "spark.sql.storeAssignmentPolicy": "legacy"
            },

            "args": ["workflow=" + WORKFLOW_NAME, "runmode=" + RUN_MODE, "LndngPth=" + LndngPth, "env=" + ENV,
                     "materializationDataset=" + MATERIALIZATION_DATASET, "RunDate=" + RUN_DATE, "cur_date=" + RUN_DATE,
                     "parentproject=" + PARENT_PROJECT, "project=" + PROJECT, "dataset=" + DATASET,
                     "BASEDIVNBR=" + BASEDIVNBR, "geo_rgn_cd=" + GEO_RGN_CD, "op_cmpny_cd=" + OP_CMPNY_CD,
                     "TgtTbl=" + TGT_TBL, "NumPartns=" + NUM_PARTNS, "enableservices=Dataquality", "runDQ=true"
                     ],

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml"
                          ]
        }
    }


repl_sku_dc_shave_dly_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_dc_shave_dly_spark_load_job', job=create_spark_job("REPL_SKU_DC_SHAVE_DLY"),
    gcp_conn_id=CONN_ID, dag=repl_sku_dc_shave_dly_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_sku_dc_shave_dly_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID,
                                               on_success_callback=success_email_function)

create_bfdms_dpaas_cluster >> repl_sku_dc_shave_dly_spark_load_job >> delete_cluster
