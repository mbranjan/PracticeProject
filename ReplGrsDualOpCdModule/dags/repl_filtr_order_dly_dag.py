from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.utils.email import send_email
from bfdms.dpaas import BFDMSDataprocSubmitJobOperator as DataprocSubmitJobOperator
from bfdms.dpaas import BFDMSDataprocDeleteClusterOperator as DataprocDeleteClusterOperator
from bfdms.dpaas import BFDMSDataprocCreateClusterOperator
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

SUCCESS_MSG = "REPL_FILTR_ORDER_DLY table load was successful"

CLUSTER_NAME = "repl-dl-ephe-repl-filtr-order-dly"

REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_filtr_order_dly_dag"
DAG_DESC = "REPL Filtr order dly spark load job"
APP_NAME = "REPL_FILTR_ORDER_DLY"

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
            "num_instances": 3,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 200
            }
        },
        "secondary_worker_config": {
            "num_instances": 0,
            "machine_type_uri": "e2-standard-8",
            "disk_config": {
                "boot_disk_size_gb": 200
            }
        },
        'lifecycle_config': {
            'auto_delete_ttl': {'seconds': 21600},
            'idle_delete_ttl': {'seconds': 1800}
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

repl_filtr_order_dly_load_dag = DAG(
    DAG_NAME,
    description=DAG_DESC,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    max_active_runs=1,
    concurrency=5,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['ReplGrsDualOpCdModule']
)

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=repl_filtr_order_dly_load_dag,
    dpaas_config=dpaas_config,
    gcp_conn_id=CONN_ID,
    trigger_rule='all_success')

SCHEMA_NAME = Variable.get("repl_filtr_order_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_filtr_order_load_dag_var", deserialize_json=True)["table_name_dly"]
# SCHEMA_NAME = "ww_repl_dl_tables"
# TABLE_NAME = "repl_filtr_order_dly"

args_wmt = ["workflow=REPL_FILTR_ORDER_DLY_WMT", "runmode=global", "OP_CMPNY_CD=WMT-US",
            "TABLE_NAME=" + TABLE_NAME, "SCHEMA_NAME=" + SCHEMA_NAME, "partition=50", "env=" + ENV
            ]

args_sams = ["workflow=REPL_FILTR_ORDER_DLY_SAMS", "runmode=global", "OP_CMPNY_CD=SAMS-US",
             "TABLE_NAME=" + TABLE_NAME, "SCHEMA_NAME=" + SCHEMA_NAME, "partition=5", "env=" + ENV
             ]


def create_spark_job(args):
    return {

        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Grs_Dual_Op_Cd_Module/ReplGrsDualOpCdModule-1.23.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.4-bundled.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkflowController",
            "properties": {
                "spark.submit.deployMode": "cluster",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.instances": "15",
                "spark.executor.cores": "4",
                "spark.executor.memory": "10g",
                "spark.driver.memory": "5g",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            },

            "args": args

        }
    }


repl_filtr_order_dly_wmt_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_filtr_order_dly_wmt_spark_load_job', job=create_spark_job(args_wmt),
    gcp_conn_id=CONN_ID, dag=repl_filtr_order_dly_load_dag, trigger_rule='all_success')

repl_filtr_order_dly_sams_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_filtr_order_dly_sams_spark_load_job', job=create_spark_job(args_sams),
    gcp_conn_id=CONN_ID, dag=repl_filtr_order_dly_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_filtr_order_dly_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

# ************* trigger file creation ********
FILE_PATH = "/grid_transient/EIMDI"
DATETIME = str(datetime.now().strftime("%Y%m%d%H%M%S"))

GEO_RGN_CD = "us"
TRIG_FILE = f"{FILE_PATH}/{GEO_RGN_CD}_{SCHEMA_NAME}_{TABLE_NAME}_{DATETIME}.trig"

trig_command = "rm  " + TRIG_FILE + "; echo 'Creating Trigger file: '; touch  " + TRIG_FILE + "; chmod 777 " + TRIG_FILE + ";"
repl_filtr_order_dly_create_trig_file = SSHOperator(
    task_id="repl_filtr_order_dly_create_trig_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command=trig_command,
    on_success_callback=success_email_function,
    dag=repl_filtr_order_dly_load_dag
)

create_bfdms_dpaas_cluster >> repl_filtr_order_dly_wmt_spark_load_job >> delete_cluster
create_bfdms_dpaas_cluster >> repl_filtr_order_dly_sams_spark_load_job >> delete_cluster
delete_cluster >> repl_filtr_order_dly_create_trig_file
