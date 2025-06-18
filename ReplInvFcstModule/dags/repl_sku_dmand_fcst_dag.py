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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import base64

sshHook = SSHHook('prod17-ssh-conn')
# edgenode_sshHook = SSHHook('edgenode-ssh-conn')

SERVICE_ACCOUNT = Variable.get("service_account")
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
SUCCESS_MSG = "REPL_SKU_DMAND_FCST table load was successful"

RUNMODE = "global"

CLUSTER_NAME = "repl-dl-ephe-repl-sku-dmand-fcst"

REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_sku_dmand_fcst"
DAG_DESC = "REPL SKU demand forecast spark load job"
APP_NAME = "REPL_SKU_DMAND_FCST"

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
            "machine_type_uri": "e2-standard-16",
            "disk_config": {
                "boot_disk_size_gb": 500
            }
        },
        "worker_config": {
            "num_instances": 15,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 500
            }
        },
        "secondary_worker_config": {
            "num_instances": 4,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 500
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

repl_sku_dmand_fcst_load_dag = DAG(
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

FILE_PATH = "/grid_transient/EIMDI"
SIGNAL_FILE_PATTERN = "Signal_FCST_DEMAND_USWMTNIGHTBATCH*_*.txt"
POLL_PERIOD = "5"
WAIT_PERIOD = "120"

# sh /u/appaorta/aorta/scripts/Aorta_file_poll_check.sh &FILE_PATH# &FILE_PATTERN# &POLL_PERIOD# &WAIT_PERIOD#
repl_sku_dmand_fcst_common_event_poll = SSHOperator(
    task_id="common_event_poll",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/Aorta_file_poll_check.sh  " + FILE_PATH + " " + SIGNAL_FILE_PATTERN + " " + POLL_PERIOD + " " + WAIT_PERIOD,
    dag=repl_sku_dmand_fcst_load_dag
)

# **************** Oldest File check Task started ******************

# sh /u/appaorta/aorta/scripts/Aorta_Get_Oldest_Bin_File_Name_Date_Time.sh &NFS_PATH# &SRC_FILE_NAME#
repl_sku_dmand_fcst_oldest_file_check = SSHOperator(
    task_id="oldest_file_check",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/Aorta_Get_Oldest_Bin_File_Name_Date_Time.sh  " + FILE_PATH + " " + SIGNAL_FILE_PATTERN,
    dag=repl_sku_dmand_fcst_load_dag
)


def decode_and_assign_variable(ti):
    command_output = str(ti.xcom_pull(task_ids='oldest_file_check'))

    SRC_SIGNAL_FILE_FULL_NAME = command_output.split("^")[0]
    SRCRCVTS = command_output.split("^")[1]
    SRCRCVDT = command_output.split("^")[2]
    DATETIME = command_output.split("^")[3]
    GRS_JOB_NAME = command_output.split("^")[4]

    ti.xcom_push(key="SRC_SIGNAL_FILE_FULL_NAME", value=SRC_SIGNAL_FILE_FULL_NAME)
    ti.xcom_push(key="SRCRCVTS", value=SRCRCVTS)
    ti.xcom_push(key="SRCRCVDT", value=SRCRCVDT)
    ti.xcom_push(key="DATETIME", value=DATETIME)
    ti.xcom_push(key="GRS_JOB_NAME", value=GRS_JOB_NAME)

    print("########### Variables inside decode_variable ############")
    print(SRC_SIGNAL_FILE_FULL_NAME)
    print(SRCRCVTS)
    print(SRCRCVDT)
    print(DATETIME)
    print(GRS_JOB_NAME)


assign_variable = PythonOperator(
    task_id='assign_variable',
    python_callable=decode_and_assign_variable,
    provide_context=True,
    dag=repl_sku_dmand_fcst_load_dag
)

# **************** File move to HDFS Task started ******************
SRC_SIGNAL_FILE_FULL_NAME = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRC_SIGNAL_FILE_FULL_NAME') }}"
DATETIME = "{{ ti.xcom_pull(task_ids='assign_variable', key='DATETIME') }}"
SRCRCVDT = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVDT') }}"
SRCRCVTS = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVTS') }}"
GRS_JOB_NAME = "{{ ti.xcom_pull(task_ids='assign_variable', key='GRS_JOB_NAME') }}"

OLDEST_FILE_DEL = FILE_PATH + "/FCST_DEMAND_DELETE_USWMTNIGHTBATCH*" + DATETIME + "_" + GRS_JOB_NAME + "*"
OLDEST_FILE_UPS = FILE_PATH + "/FCST_DEMAND_UPSERT_USWMTNIGHTBATCH*" + DATETIME + "_" + GRS_JOB_NAME + "*"
LANDING_HDFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/landing"
LOAD_TYPE = "incremental"
COUNTRY_CODE = "us"
SCHEMA_NAME = Variable.get("repl_sku_dmand_fcst_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_sku_dmand_fcst_load_dag_var", deserialize_json=True)["table_name"]
# SCHEMA_NAME = "ww_repl_dl_secure"
# TABLE_NAME = "repl_sku_dmand_fcst"
TABLE_PATH_DEL = TABLE_NAME + "/delete"
TABLE_PATH_UPS = TABLE_NAME + "/upsert"

# sh /u/appaorta/aorta/scripts/nfs_cp_resources/Bin_File_GCS_Put.sh &OLDEST_FILE# &LANDING_HDFS_PATH# &LOAD_TYPE# &COUNTRY_CODE# &SCHEMA_NAME# &TABLE_PATH_DELETE# &SRCRCVDT#
repl_sku_dmand_fcst_del_file_move_to_hdfs = SSHOperator(
    task_id="file_move_to_hdfs_del",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/nfs_cp_resources/Bin_File_GCS_Put.sh  " + OLDEST_FILE_DEL + " " + LANDING_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_DEL + " " + SRCRCVDT,
    dag=repl_sku_dmand_fcst_load_dag
)

# sh /u/appaorta/aorta/scripts/nfs_cp_resources/Bin_File_GCS_Put.sh &OLDEST_FILE# &LANDING_HDFS_PATH# &LOAD_TYPE# &COUNTRY_CODE# &SCHEMA_NAME# &TABLE_PATH_UPSERT# &SRCRCVDT#
repl_sku_dmand_fcst_ups_file_move_to_hdfs = SSHOperator(
    task_id="file_move_to_hdfs_ups",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/nfs_cp_resources/Bin_File_GCS_Put.sh  " + OLDEST_FILE_UPS + " " + LANDING_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_UPS + " " + SRCRCVDT,
    dag=repl_sku_dmand_fcst_load_dag
)
# ******************* Spark Job task started **********************

LAKE_BUCKET = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd"
LOCATION_UPS = LANDING_HDFS_PATH + "/incremental/us/" + SCHEMA_NAME + "/" + TABLE_PATH_UPS + "/src_rcv_dt"
LOCATION_DEL = LANDING_HDFS_PATH + "/incremental/us/" + SCHEMA_NAME + "/" + TABLE_PATH_DEL + "/src_rcv_dt"
GEO_RGN_CD = "US"

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=repl_sku_dmand_fcst_load_dag,
    dpaas_config=dpaas_config,
    gcp_conn_id=CONN_ID,
    trigger_rule='all_success')


def create_spark_job(WORKFLOW_NAME):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Inv_Fcst_Module/ReplInvFcstModule-1.40.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.8-bundled.jar",
                              REPL_JAR_PATH + "hive-serde-ebcdic2ascii-0.0.1-SNAPSHOT-jar-with-dependencies.jar",
                              REPL_JAR_PATH + "hive-contrib-2.3.7.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkflowController",
            "properties": {
                "spark.submit.deployMode": "cluster",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.instances": "200",
                "spark.executor.cores": "3",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.shuffle.partitions": "800",
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

            "args": ["workflow=" + WORKFLOW_NAME, "runmode=" + RUNMODE, "location_ups=" + LOCATION_UPS,
                     "location_del=" + LOCATION_DEL, "srcrcvdt=" + SRCRCVDT, "src_rcv_ts=" + SRCRCVTS,
                     "Schema=" + SCHEMA_NAME, "Table=" + TABLE_NAME, "CatlgTable=" + SCHEMA_NAME + "." + TABLE_NAME,
                     "runDQ=true", "DqTable=stg_ww_repl_dl_secure.repl_sku_dmnd_fcst_affected_partition",
                     "enableservices=Dataquality,Audit", "env=" + ENV],

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml"
                          ]
        }
    }


repl_sku_dmand_fcst_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_dmand_fcst_spark_load_job', job=create_spark_job("ReplSkuDmandFcstWF"),
    gcp_conn_id=CONN_ID, dag=repl_sku_dmand_fcst_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_sku_dmand_fcst_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

# AORTA_RIVER_HOME = '/edge_data/udp/data-pipeline-dpaas-lw-10.5'
# SQOOP_HOME = '/edge_data/dpaaslibs/bfdms-ien/dp1.5/lib/sqoop-1.4.7/'
# YAML_FILE = '/edge_data/code/svcmdsedat/availability/resources/yamls/repl_sku_dmand_fcst/uswm_mdsegcp_repl_sku_dmand_fcst_target_load.yaml'
# LOAD_NAME = 'ReplSkuDmandFcst'
# GCP_PROJECT = "wmt-bfdms-mdsedlprd"
# CLUSTER_REGION = "us-central1"
# CLUSTER_NAME = "repl-dl-ephe-mdse-sku-invt-adj"

# gcloud config set project &GCP_PROJECT#
# source ~/.profile
# export SQOOP_HOME='/edge_data/dpaaslibs/bfdms-ien/dp1.5/lib/sqoop-1.4.7/'
# export AORTA_RIVER_HOME='/edge_data/udp/data-pipeline-dpaas-lw-10.5'
# export YAML_PATH='&YAML_PATH#'
#
# sh $AORTA_RIVER_HOME/run-data-pipeline.sh -yaml $YAML_PATH/&YAML_FILE# -connectionFile /edge_data/code/svcmdsedat/availability/resources/connections/connection.yaml -streamMode false -loadName MdseSkuInvt -env prod -runner local &key_value_params# -distribution DPAAS_NATIVE -dpaasGcpProject &GCP_PROJECT# -dpaasGcpProjectRegion &CLUSTER_REGION# -dpaasGcpCluster &CLUSTER_NAME#

# # ***************** Creating External Tables *********************
# repl_sku_dmand_fcst_create_ext_table = SSHOperator(
#     task_id="create_external_table",
#     ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
#     command=f"sh {AORTA_RIVER_HOME}/run-data-pipeline.sh  -yaml {YAML_FILE} -connectionFile /edge_data/code/svcmdsedat/availability/resources/connections/connection.yaml -streamMode false -loadName {LOAD_NAME} -env {env} -runner local -distribution DPAAS_NATIVE -dpaasGcpProject {GCP_PROJECT} -dpaasGcpProjectRegion {CLUSTER_REGION} -dpaasGcpCluster {CLUSTER_NAME}",
#     dag=repl_sku_dmand_fcst_load_dag
# )

# **************** HDFS File archive Task started ******************

ARCHIVE_HDFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/raw"

# sh /edge_data/code/svcmdsedat/availability/resources/scripts/GCP_Archive_SourceFile.sh &LANDING_GCS_PATH# &ARCHIVE_GCS_PATH# &LOAD_TYPE# &COUNTRY_CODE# &SCHEMA_NAME# &TABLE_NAME# &SRCRCVDT#
repl_sku_dmand_fcst_del_file_archive_in_hdfs = SSHOperator(
    task_id="file_archive_in_hdfs_del",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/scripts/GCP_Archive_SourceFile.sh  " + LANDING_HDFS_PATH + " " + ARCHIVE_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_DEL + " " + SRCRCVDT,
    dag=repl_sku_dmand_fcst_load_dag
)

repl_sku_dmand_fcst_ups_file_archive_in_hdfs = SSHOperator(
    task_id="file_archive_in_hdfs_ups",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/scripts/GCP_Archive_SourceFile.sh  " + LANDING_HDFS_PATH + " " + ARCHIVE_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_UPS + " " + SRCRCVDT,
    dag=repl_sku_dmand_fcst_load_dag
)
# **************** Delete Oldest file from NFS mount Task started ******************

repl_sku_dmand_fcst_delete_oldest_file_del = SSHOperator(
    task_id="delete_oldest_del_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  " + OLDEST_FILE_DEL + ";",
    dag=repl_sku_dmand_fcst_load_dag
)

repl_sku_dmand_fcst_delete_oldest_file_ups = SSHOperator(
    task_id="delete_oldest_upsert_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  " + OLDEST_FILE_UPS + ";",
    dag=repl_sku_dmand_fcst_load_dag
)

# **************** Delete Signal file from NFS mount Task started ******************
repl_sku_dmand_fcst_delete_signal_file = SSHOperator(
    task_id="delete_signal_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  " + SRC_SIGNAL_FILE_FULL_NAME + ";",
    dag=repl_sku_dmand_fcst_load_dag
)

repl_sku_dmand_fcst_common_event_poll >> repl_sku_dmand_fcst_oldest_file_check >> assign_variable >> repl_sku_dmand_fcst_del_file_move_to_hdfs >> repl_sku_dmand_fcst_ups_file_move_to_hdfs >> create_bfdms_dpaas_cluster >> repl_sku_dmand_fcst_spark_load_job
repl_sku_dmand_fcst_spark_load_job >> repl_sku_dmand_fcst_create_ext_table >> delete_cluster >> repl_sku_dmand_fcst_del_file_archive_in_hdfs >> repl_sku_dmand_fcst_ups_file_archive_in_hdfs >> repl_sku_dmand_fcst_delete_oldest_file_del >> repl_sku_dmand_fcst_delete_oldest_file_ups >> repl_sku_dmand_fcst_delete_signal_file
