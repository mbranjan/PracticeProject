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

SUCCESS_MSG = "repl_sku_tm_ld table load was successful"

CLUSTER_NAME = "repl-dl-ephe-repl-sku-ld-tm"
REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_sku_tm_ld_dag"
DAG_DESC = "repl_sku_tm_ld table load job"
APP_NAME = "repl_sku_tm_ld"

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
                "boot_disk_size_gb": 100
            }
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 300
            }
        },
        "secondary_worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 100
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

repl_sku_tm_ld_dag = DAG(
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


def create_spark_job(spark_properties, job_args):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Full_Load_Module/ReplFullLoadModule-1.6.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.4-bundled.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkflowController",
            "properties": spark_properties,

            "args": job_args,

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml"
                          ]
        }
    }


# SCHEMA = "ww_repl_dl_secure"
# TABLE = "repl_sku_ld_tm"
SCHEMA_NAME = Variable.get("repl_sku_tm_ld_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_sku_tm_ld_dag_var", deserialize_json=True)["table_name"]
OP_CMPNY_CD = "WMT-US"
RUN_MODE = "global"
GEO_RGN_CD = "US"
CURR_DATE = str(date.today())
TGT_TBL = SCHEMA_NAME + "." + TABLE_NAME

WF_NAME1 = "ReplSkuLdTmStgWorkflow"
WF_NAME2 = "ReplSkuLdTmSTLTWorkflow"
WF_NAME3 = "ReplSkuLdTmFFCWorkflow"
WF_NAME4 = "ReplSkuLdTmWhseLtWorkflow"
WF_NAME5 = "ReplSkuLdTmWorkflow"

repl_sku_ld_tm_stg_spark_properties = {
    "spark.driver.memory": "10g",
    "spark.executor.memory": "8g",
    "spark.executor.cores": "4",
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.executor.instances": "45",
    "spark.submit.deployMode": "cluster",
    "spark.dynamicAllocation.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}

repl_sku_ld_tm_stl_spark_properties = {
    "spark.driver.memory": "10g",
    "spark.executor.cores": "4",
    "spark.executor.memory": "10g",
    "spark.executor.instances": "20",
    "spark.submit.deployMode": "cluster",
    "spark.dynamicAllocation.enabled": "false",
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.shuffle.partitions": "200"
}

repl_sku_ld_tm_fcc_spark_properties = {
    "spark.driver.memory": "5g",
    "spark.executor.cores": "4",
    "spark.executor.memory": "10g",
    "spark.executor.instances": "10",
    "spark.submit.deployMode": "cluster",
    "spark.dynamicAllocation.enabled": "false",
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.autoBroadcastJoinThreshold": "-1"
}

repl_sku_ld_tm_whse_lt_spark_properties = {
    "spark.driver.memory": "10g",
    "spark.executor.cores": "4",
    "spark.executor.memory": "10g",
    "spark.executor.instances": "15",
    "spark.submit.deployMode": "cluster",
    "spark.dynamicAllocation.enabled": "false",
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.autoBroadcastJoinThreshold": "-1"
}

repl_sku_ld_tm_spark_properties = {
    "spark.driver.memory": "15g",
    "spark.executor.cores": "4",
    "spark.executor.memory": "15g",
    "spark.executor.instances": "45",
    "spark.submit.deployMode": "cluster",
    "spark.dynamicAllocation.enabled": "false",
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.driver.extraJavaOptions": "-Dudp.job.file=archetype_spark.yaml -Dudp.conn.file=connection.yaml"
}

args_1 = ["workflow=" + WF_NAME1, "env=" + ENV, "runmode=" + RUN_MODE, "schema=" + SCHEMA_NAME,
          "table=" + TABLE_NAME, "op_cmpny_cd=" + OP_CMPNY_CD
          ]

args_2 = ["workflow=" + WF_NAME2, "runmode=" + RUN_MODE, "schema=" + SCHEMA_NAME, "table=" + TABLE_NAME,
          "op_cmpny_cd=" + OP_CMPNY_CD, "curr_dt=" + CURR_DATE, "env=" + ENV
          ]

args_3 = ["workflow=" + WF_NAME3, "runmode=" + RUN_MODE, "schema=" + SCHEMA_NAME, "table=" + TABLE_NAME,
          "op_cmpny_cd=" + OP_CMPNY_CD, "curr_dt=" + CURR_DATE, "env=" + ENV
          ]

args_4 = ["workflow=" + WF_NAME4, "runmode=" + RUN_MODE, "schema=" + SCHEMA_NAME, "table=" + TABLE_NAME,
          "op_cmpny_cd=" + OP_CMPNY_CD, "curr_dt=" + CURR_DATE, "env=" + ENV
          ]

args_5 = ["workflow=" + WF_NAME5, "runmode=" + RUN_MODE, "schema=" + SCHEMA_NAME, "table=" + TABLE_NAME,
          "op_cmpny_cd=" + OP_CMPNY_CD, "geo_region_cd=" + GEO_RGN_CD, "target_table=" + TGT_TBL,
          "env=" + ENV, "enableservices=Dataquality", "runDQ=true"
          ]

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=repl_sku_tm_ld_dag,
    dpaas_config=dpaas_config,
    gcp_conn_id=CONN_ID,
    trigger_rule='all_success')

repl_sku_ld_tm_stg_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_ld_tm_stg_spark_load_job',
    job=create_spark_job(repl_sku_ld_tm_stg_spark_properties, args_1),
    gcp_conn_id=CONN_ID, dag=repl_sku_tm_ld_dag, trigger_rule='all_success')

repl_sku_ld_tm_stl_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_ld_tm_stl_spark_load_job',
    job=create_spark_job(repl_sku_ld_tm_stl_spark_properties, args_2),
    gcp_conn_id=CONN_ID, dag=repl_sku_tm_ld_dag, trigger_rule='all_success')

repl_sku_ld_tm_fcc_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_ld_tm_fcc_spark_load_job',
    job=create_spark_job(repl_sku_ld_tm_fcc_spark_properties, args_3),
    gcp_conn_id=CONN_ID, dag=repl_sku_tm_ld_dag, trigger_rule='all_success')

repl_sku_ld_tm_whse_lt_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_ld_tm_whse_lt_spark_load_job',
    job=create_spark_job(repl_sku_ld_tm_whse_lt_spark_properties, args_4),
    gcp_conn_id=CONN_ID, dag=repl_sku_tm_ld_dag, trigger_rule='all_success')

repl_sku_ld_tm_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_ld_tm_spark_load_job',
    job=create_spark_job(repl_sku_ld_tm_spark_properties, args_5),
    gcp_conn_id=CONN_ID, dag=repl_sku_tm_ld_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_sku_tm_ld_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID,
                                               on_success_callback=success_email_function
                                               )

create_bfdms_dpaas_cluster >> repl_sku_ld_tm_stg_spark_load_job
repl_sku_ld_tm_stg_spark_load_job >> repl_sku_ld_tm_stl_spark_load_job >> repl_sku_ld_tm_spark_load_job
repl_sku_ld_tm_stg_spark_load_job >> repl_sku_ld_tm_fcc_spark_load_job >> repl_sku_ld_tm_spark_load_job
repl_sku_ld_tm_stg_spark_load_job >> repl_sku_ld_tm_whse_lt_spark_load_job >> repl_sku_ld_tm_spark_load_job
repl_sku_ld_tm_spark_load_job >> delete_cluster
