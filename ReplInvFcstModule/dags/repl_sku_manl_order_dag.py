from datetime import datetime, timedelta
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

SERVICE_ACCOUNT = Variable.get("service_account")
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
SUCCESS_MSG = "repl_sku_manl_order table load was successful"

RUNMODE = "global"

CLUSTER_NAME = "repl-dl-ephe-repl-sku-manl-order"

REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_sku_manl_order_dag"
DAG_DESC = "REPL SKU MANL ORDER table load job"
APP_NAME = "REPL_SKU_MANL"

SCHEMA_NAME = Variable.get("repl_sku_manl_order_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_sku_manl_order_load_dag_var", deserialize_json=True)["table_name"]
TGT_TBL = SCHEMA_NAME + "." + TABLE_NAME
PARENT_PROJECT = "wmt-dca-repl-dl-prod"
PROJECT = "wmt-dca-repl-dl-prod"
DATASET = "US_WM_REPL_VM"
VIEW = "store_manl_order_view"
MATERIALIZATION_DATASET = "US_REPL_BQ_STG"

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
            "num_instances": 4,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 300
            }
        },
        "secondary_worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 300
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

repl_sku_manl_order_load_dag = DAG(
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

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=repl_sku_manl_order_load_dag,
    dpaas_config=dpaas_config,
    gcp_conn_id=CONN_ID)


def create_spark_job(WORKFLOW_NAME):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Inv_Fcst_Module/ReplInvFcstModule-1.33.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.4-bundled.jar",
                              REPL_JAR_PATH + "hive-serde-ebcdic2ascii-0.0.1-SNAPSHOT-jar-with-dependencies.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkflowController",
            "properties": {
                "spark.driver.memory": "12G",
                "spark.executor.memory": "12g",
                "spark.executor.cores": "4",
                "spark.submit.deployMode": "cluster",
                "spark.sql.storeAssignmentPolicy": "legacy",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.instances": "50",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.legacy.charVarcharAsString": "true",
                "spark.sql.shuffle.partitions": "400",
                "spark.yarn.executor.memoryOverhead": "3g",
                "spark.yarn.driver.memoryOverhead": "3g",
                "spark.yarn.maxAppAttempts": "1",
                "spark.driver.extraJavaOptions": "-Dudp.conn.file=connection.yaml -Dudp.job.file=archetype_spark.yaml"

            },

            "args": ["workflow=" + WORKFLOW_NAME, "runmode=global", "BASEDIVNBR=1", "geo_region_cd=US",
                     "op_cmpny_cd=WMT-US", "SCHEMA_NAME=" + SCHEMA_NAME, "TABLE_NAME=" + TABLE_NAME, "runDQ=true",
                     "env=" + ENV, "TgtTable=" + TGT_TBL, "NumPartns=5",
                     "CatlgTable=" + TGT_TBL, "enableservices=DATAQUALITY,AUDIT",
                     "parentproject=" + PARENT_PROJECT, "project=" + PROJECT, "dataset=" + DATASET, "view=" + VIEW,
                     "materializationDataset=" + MATERIALIZATION_DATASET
                     ],

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml"
                          ]
        }
    }


repl_sku_manl_order_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_manl_order_spark_load_job', job=create_spark_job("REPL_SKU_MANL_ORDER_BQ"),
    gcp_conn_id=CONN_ID, dag=repl_sku_manl_order_load_dag, trigger_rule='all_success',
    on_success_callback=success_email_function)

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_sku_manl_order_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

create_bfdms_dpaas_cluster >> repl_sku_manl_order_spark_load_job >> delete_cluster
