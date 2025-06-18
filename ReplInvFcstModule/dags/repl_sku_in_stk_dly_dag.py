from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.utils.email import send_email
from bfdms.dpaas import BFDMSDataprocSubmitJobOperator as DataprocSubmitJobOperator
from bfdms.dpaas import BFDMSDataprocDeleteClusterOperator as DataprocDeleteClusterOperator
from bfdms.dpaas import BFDMSDataprocCreateClusterOperator
from airflow.utils.dates import days_ago
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import base64
from depflow.operators.dependency_tracker_operator import DependencyTrackerOperator

sshHook = SSHHook('prod17-ssh-conn')
edgenode_sshHook = SSHHook('edgenode-ssh-conn')

SERVICE_ACCOUNT = Variable.get("service_account")
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
AF_INSTANCE_NAME = Variable.get("common_variables", deserialize_json=True)["af_instance_name"]
SUCCESS_MSG = "REPL_SKU_IN_STK_DLY table load was successful"

RUNMODE = "global"

CLUSTER_NAME = "repl-dl-ephe-repl-sku-in-stk-dly"

REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_sku_in_stk_dly_dag"
DAG_DESC = "Repl SKU instock daily spark load job"
APP_NAME = "REPL_SKU_IN_STK_DLY"

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
                "boot_disk_size_gb": 300
            }
        },
        "worker_config": {
            "num_instances": 6,
            "machine_type_uri": "n1-highmem-32",
            "disk_config": {
                "boot_disk_size_gb": 300
            }
        },
        "secondary_worker_config": {
            "num_instances": 3,
            "machine_type_uri": "n1-highmem-32",
            "disk_config": {
                "boot_disk_size_gb": 300
            }
        },
        'lifecycle_config': {
            'auto_delete_ttl': {'seconds': 21600},
            'idle_delete_ttl': {'seconds': 2700}
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

repl_sku_in_stk_dly_load_dag = DAG(
    DAG_NAME,
    description=DAG_DESC,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    max_active_runs=1,
    concurrency=5,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['ReplInvFcstModule']
)

# **************** File poll check Task started ******************

today = date.today().strftime("%Y%m%d")
SRC_FILE_NAME = "MF-LMNX.N01.REPL.SKU.IN.STK.DLY.OVERLAY.TRG." + today + "*"
NFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/LUMINEX/AVAILABILITY/REPL-SKU-IN-STK-DLY"
POLL_PERIOD = "3"
MAX_PERIOD = "600"

# sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh &NFS_PATH# &SRC_FILE_NAME# &POLL_PERIOD# &MAX_PERIOD#
repl_sku_in_stk_dly_gcp_bucket_file_poll = SSHOperator(
    task_id="gcp_bucket_file_poll",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh  " + NFS_PATH + " '" + SRC_FILE_NAME + "' " + POLL_PERIOD + " " + MAX_PERIOD,
    dag=repl_sku_in_stk_dly_load_dag
)

delete_command = "gcloud auth activate-service-account --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; gsutil -m rm -r " + NFS_PATH + "/" + SRC_FILE_NAME + " | sh -s "

repl_sku_in_stk_dly_delete_trigger_file = BashOperator(
    task_id="repl_sku_in_stk_dly_delete_trigger_file",
    bash_command=delete_command,
    dag=repl_sku_in_stk_dly_load_dag,
    trigger_rule='all_success'
)

# ******************* Spark Job task started **********************

OP_CMPNY_CD = "WMT-US"
GEO_RGN_CD = "US"
# LOAD_TYPE = "incremental"
# COUNTRY_CODE = "us"

SCHEMA_NAME = Variable.get("repl_sku_in_stk_dly_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_sku_in_stk_dly_load_dag_var", deserialize_json=True)["table_name"]
# SCHEMA_NAME = "ww_repl_dl_secure"
# TABLE_NAME = "repl_sku_in_stk_dly"

PARENT_PROJECT = "wmt-dca-repl-dl-prod"
PROJECT = "wmt-dca-repl-dl-prod"
DATASET = "ei_inventory_metrics"
VIEW = "store_manl_order_view"
MATERIALIZATION_DATASET = "US_REPL_BQ_STG"

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=repl_sku_in_stk_dly_load_dag,
    dpaas_config=dpaas_config,
    gcp_conn_id=CONN_ID,
    trigger_rule='all_success')


def create_spark_job(WORKFLOW_NAME):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Inv_Fcst_Module/ReplInvFcstModule-1.55.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.4-bundled.jar",
                              REPL_JAR_PATH + "hive-serde-ebcdic2ascii-0.0.1-SNAPSHOT-jar-with-dependencies.jar",
                              REPL_JAR_PATH + "hive-contrib-2.3.7.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkflowController",
            "properties": {
                "spark.sql.storeAssignmentPolicy": "legacy",
                "spark.submit.deployMode": "cluster",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.instances": "100",
                "spark.executor.cores": "3",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.shuffle.partitions": "300",
                "spark.executor.memory": "12g",
                "spark.driver.memory": "12g",
                "spark.sql.files.maxPartitionBytes": "94371840",
                "spark.scheduler.mode": "FIFO",
                "spark.executor.memoryOverhead": "3g",
                "spark.driver.memoryOverhead": "3g",
                "spark.executor.heartbeatInterval": "10000000",
                "spark.network.timeout": "10000001",
                "spark.shuffle.io.retryWait": "60s",
                "spark.shuffle.io.maxRetries": "2",
                "spark.yarn.maxAppAttempts": "1",
                "spark.driver.extraJavaOptions": "-Dudp.conn.file=connection.yaml -Dudp.job.file=archetype_spark.yaml"

            },

            "args": ["workflow=" + WORKFLOW_NAME, "runmode=global", "Schema=" + SCHEMA_NAME, "Table=" + TABLE_NAME,
                     "runDQ=true", "CatlgTable=" + SCHEMA_NAME + "." + TABLE_NAME, "enableservices=Dataquality,Audit",
                     "TgtTable=" + SCHEMA_NAME + "." + TABLE_NAME, "env=" + ENV, "geo_region_cd=" + GEO_RGN_CD,
                     "extTable=stg_ww_repl_dl_secure.stg_us_wm_repl_sku_in_stk_dly_inc_ext", "NumPartns=100",
                     "op_cmpny_cd=" + OP_CMPNY_CD, "parentproject=" + PARENT_PROJECT, "project=" + PROJECT,
                     "dataset=" + DATASET, "materializationDataset=" + MATERIALIZATION_DATASET],

            "file_uris": [
                REPL_FILE_PATH + "log4j.properties",
                REPL_FILE_PATH + "archetype_spark.yaml",
                REPL_FILE_PATH + "connection.yaml"
            ]
        }
    }


repl_sku_in_stk_dly_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_in_stk_dly_spark_load_job', job=create_spark_job("REPL_SKU_IN_STK_DLY_OVRLY"),
    gcp_conn_id=CONN_ID, dag=repl_sku_in_stk_dly_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_sku_in_stk_dly_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

TOUCH_DATE = date.today().strftime("%Y-%m-%d")
TOUCH_FILE = "gs://repl-prod-handshake-files/repl_sku_in_stk_dly/DONE@" + TOUCH_DATE
create_touch_file_command = "gcloud auth activate-service-account --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; export TZ='America/Chicago'; gsutil cp /dev/null " + TOUCH_FILE + " | sh -s "

repl_sku_in_stk_dly_create_touch_file = BashOperator(
    task_id="repl_sku_in_stk_dly_create_touch_file",
    bash_command=create_touch_file_command,
    dag=repl_sku_in_stk_dly_load_dag,
    trigger_rule='all_success'
)

TRIG_FILE_PATH = "/grid_transient/EIMDI/MERCWB"
CMPNY_CD = ""
SRC_TABLE = "repl_sku_in_stk_dly"
TGT_TABLE = "repl_sku_invt_dly"
repl_sku_in_stk_dly_create_trig_file = SSHOperator(
    task_id="repl_sku_in_stk_dly_create_trig_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/Aorta_Create_Trigger_File.sh  " + TRIG_FILE_PATH + " " + CMPNY_CD + " " + TGT_TABLE + " " + SRC_TABLE + ";",
    dag=repl_sku_in_stk_dly_load_dag,
    trigger_rule='all_success'
)

repl_sku_in_stk_dly_upstream_beacon = DependencyTrackerOperator(task_id="repl_sku_in_stk_dly_upstream_beacon", gcp_conn_id=CONN_ID,
                                            af_instance_name=AF_INSTANCE_NAME,
                                            project_id="wmt-551a0f2e3442564c92dd859af7",
                                            dag=repl_sku_in_stk_dly_load_dag)

repl_sku_in_stk_dly_gcp_bucket_file_poll >> repl_sku_in_stk_dly_delete_trigger_file >> create_bfdms_dpaas_cluster >> repl_sku_in_stk_dly_spark_load_job >> delete_cluster >> repl_sku_in_stk_dly_create_touch_file >> repl_sku_in_stk_dly_create_trig_file
delete_cluster >> repl_sku_in_stk_dly_upstream_beacon