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
from bfdms.dpaas import GetDefaultRegionOperator
from dca.operators.job import DCASubmitJobOperator
from dca.operators.clusters import DCACreateClusterOperator
import os
import pytz
from kubernetes.client import models as k8s
from airflow.operators.python_operator import BranchPythonOperator

cst_tz = pytz.timezone("America/Chicago")

sshHook = SSHHook('prod17-ssh-conn')

CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
SUCCESS_MSG = "	repl_clust_fcst_wk_conv table load was successful"

CLUSTER_NAME = "repl-dl-ephe-repl-clust-fcst-wk-conv"
ARTIFACTS_BUCKET = Variable.get("common_variables", deserialize_json=True)["artifacts_bucket"]
FILE_NAME = str(os.path.basename(__file__).split(".")[0])[4:]

dr_dpaas_config = eval(Variable.get("dr_dpaas_config"))
REGION = GetDefaultRegionOperator.get_region(dr_dpaas_config)

DAG_NAME = "dag-" + FILE_NAME
DAG_DESC = "repl_clust_fcst_wk_conv spark load job"
APP_NAME = "repl_clust_fcst_wk_conv"


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

repl_clust_fcst_wk_conv_load_dag = DAG(
    DAG_NAME,
    description=DAG_DESC,
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 8, 1, tzinfo=cst_tz),
    max_active_runs=1,
    concurrency=5,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['ReplGrsDualOpCdModule']
)

# **************** File poll check Task started ******************
FILE_PATH = "/grid_transient/EIMDI"
SIGNAL_FILE_PATTERN = "Signal_GRS_RS_CLUST_FCST_WK_CONV*.txt"
POLL_PERIOD = "5"
WAIT_PERIOD = "240"

repl_clust_fcst_wk_conv_common_event_poll = SSHOperator(
    task_id="common_event_poll",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/Aorta_file_poll_check.sh  " + FILE_PATH + " " + SIGNAL_FILE_PATTERN + " " + POLL_PERIOD + " " + WAIT_PERIOD,
    dag=repl_clust_fcst_wk_conv_load_dag
)

# **************** Oldest File check Task started ******************
repl_clust_fcst_wk_conv_oldest_file_check = SSHOperator(
    task_id="oldest_file_check",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/Aorta_Get_Oldest_Bin_File_Name_Date_Time.sh  " + FILE_PATH + " " + SIGNAL_FILE_PATTERN,
    dag=repl_clust_fcst_wk_conv_load_dag
)


def decode_and_assign_variable(ti):
    output = str(ti.xcom_pull(task_ids='oldest_file_check'))
    command_output = base64.b64decode(output).decode('utf-8')
    print("#######################################################")
    print(command_output)

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
    dag=repl_clust_fcst_wk_conv_load_dag
)

# **************** File move to HDFS Task started ******************
SRC_SIGNAL_FILE_FULL_NAME = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRC_SIGNAL_FILE_FULL_NAME') }}"
DATETIME = "{{ ti.xcom_pull(task_ids='assign_variable', key='DATETIME') }}"
SRCRCVDT = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVDT') }}"
SRCRCVTS = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVTS') }}"

OLDEST_FILE_INSERT = FILE_PATH + "/RS_CLUST_FCST_WK_CONV_INSERTS_" + DATETIME + "_JWUSRG3U*"
OLDEST_FILE_UPDATE = FILE_PATH + "/RS_CLUST_FCST_WK_CONV_UPDATES_" + DATETIME + "_JWUSRG3U*"
LANDING_HDFS_PATH = Variable.get("LANDING_HDFS_PATH")
ARCHIVE_HDFS_PATH = Variable.get("ARCHIVE_HDFS_PATH")
LOAD_TYPE = "incremental"
COUNTRY_CODE = "us"
# OP_CMPNY_CD = "SAMS-US"
SCHEMA_NAME = Variable.get("repl_clust_fcst_wk_conv_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_clust_fcst_wk_conv_dag_var", deserialize_json=True)["table_name"]
# SCHEMA_NAME = "ww_repl_dl_secure"
# TABLE_NAME = "repl_clust_fcst_wk_conv"
TABLE_PATH_INSERT = TABLE_NAME + "/insert"
TABLE_PATH_UPDATE = TABLE_NAME + "/update"

repl_clust_fcst_wk_conv_insert_file_move_to_hdfs = SSHOperator(
    task_id="repl_clust_fcst_wk_conv_insert_file_move_to_hdfs",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/nfs_cp_resources/file_move_size_check_v2.sh  '" + OLDEST_FILE_INSERT + "' " + LANDING_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_INSERT + " " + SRCRCVDT,
    dag=repl_clust_fcst_wk_conv_load_dag
)

repl_clust_fcst_wk_conv_update_file_move_to_hdfs = SSHOperator(
    task_id="repl_clust_fcst_wk_conv_update_file_move_to_hdfs",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/nfs_cp_resources/file_move_size_check_v2.sh  '" + OLDEST_FILE_UPDATE + "' " + LANDING_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_UPDATE + " " + SRCRCVDT,
    dag=repl_clust_fcst_wk_conv_load_dag
)


def check_output(ti):
    output1 = str(ti.xcom_pull(task_ids='repl_clust_fcst_wk_conv_insert_file_move_to_hdfs'))
    command_output1 = base64.b64decode(output1).decode('utf-8')

    output2 = str(ti.xcom_pull(task_ids='repl_clust_fcst_wk_conv_update_file_move_to_hdfs'))
    command_output2 = base64.b64decode(output2).decode('utf-8')

    print("#######################################################")
    print(command_output1)
    print(command_output2)
    file_size_flag = 0
    if "Zero Byte File received" in command_output1:
        file_size_flag = file_size_flag + 1

    if "Zero Byte File received" in command_output2:
        file_size_flag = file_size_flag + 1

    ti.xcom_push(key="FILE_SIZE_FLAG", value=file_size_flag)


zero_byte_file_check = PythonOperator(
    task_id='zero_byte_file_check',
    python_callable=check_output,
    provide_context=True,
    dag=repl_clust_fcst_wk_conv_load_dag
)

FILE_SIZE_FLAG = "{{ ti.xcom_pull(task_ids='zero_byte_file_check', key='FILE_SIZE_FLAG') }}"


def task_decider(**context):
    # Logic to determine which branch to take
    if FILE_SIZE_FLAG == 2:
        return 'create_bfdms_dpaas_cluster'
    else:
        return 'repl_clust_fcst_wk_conv_delete_file_archive_in_hdfs'


branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=task_decider,
    dag=repl_clust_fcst_wk_conv_load_dag,
)
# ******************* Spark Job task started **********************

# GCP_BUCKET = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd"
# GCP_LOCATION = LANDING_HDFS_PATH + "/incremental/us/" + SCHEMA_NAME + "/" + TABLE_NAME + "/sams_us/"
# GEO_RGN_CD = "US"

cluster_configs = {
    "config": f"{ARTIFACTS_BUCKET}/cluster_config/{FILE_NAME}-cluster-config.json",
    "common_config": f"{ARTIFACTS_BUCKET}/cluster_config/common-cluster-config-mdsereplprd.json"
}

spark_config = {
    "config": f"{ARTIFACTS_BUCKET}/spark_config/{FILE_NAME}-spark-config.json",
    "common_config": f"{ARTIFACTS_BUCKET}/spark_config/common-spark-config.json"
}

# args = [f"TgtTbl={TGT_TBL}"]


create_bfdms_dpaas_cluster = DCACreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME,
    location=REGION,
    gcp_conn_id=CONN_ID,
    configs=cluster_configs,
    dag=repl_clust_fcst_wk_conv_load_dag,
    trigger_rule='all_success',
    env=ENV)


def create_spark_job(WORKFLOW_NAME):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Grs_Dual_Op_Cd_Module/ReplGrsDualOpCdModule-1.23.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.4-bundled.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkflowController",
            "properties": {
                "spark.sql.autoBroadcastJoinThreshold": "-1",
                "spark.submit.deployMode": "cluster",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.instances": "5",
                "spark.executor.cores": "4",
                "spark.driver.memory": "5g",
                "spark.executor.memory": "10g",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.driver.extraJavaOptions": "-Dudp.conn.file=connection.yaml -Dudp.job.file=archetype_spark.yaml"
            },

            "args": ["workflow=" + WORKFLOW_NAME, "runmode=global", "GCP_Location=" + GCP_LOCATION,
                     "SRC_RCV_DT=" + SRCRCVDT, "SRC_RCV_TS=" + SRCRCVTS, "OP_CMPNY_CD=" + OP_CMPNY_CD,
                     "TABLE_NAME=" + TABLE_NAME, "SCHEMA_NAME=" + SCHEMA_NAME, "partition=4", "run_dq=true",
                     "env=" + ENV, "enableservices=Dataquality"
                     ],

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml"
                          ]
        }
    }


repl_clust_fcst_wk_conv_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_clust_fcst_wk_conv_spark_load_job', job=create_spark_job("repl_clust_fcst_wk_conv"),
    gcp_conn_id=CONN_ID, dag=
    repl_clust_fcst_wk_conv_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=
                                               repl_clust_fcst_wk_conv_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

# **************** HDFS File archive Task started ******************
archive_command_delete = "gcloud auth activate-service-account --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; gsutil cat gs://dca-repl-airflow-dag/files/GCP_Archive_SourceFile.sh  | sh -s " + LANDING_HDFS_PATH + " " + ARCHIVE_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_DELETE + " " + SRCRCVDT

repl_clust_fcst_wk_conv_delete_file_archive_in_hdfs = BashOperator(
    task_id="repl_clust_fcst_wk_conv_delete_file_archive_in_hdfs",
    bash_command=archive_command_delete,
    dag=
    repl_clust_fcst_wk_conv_load_dag,
    trigger_rule='all_success'
)

archive_command_upsert = "gcloud auth activate-service-account --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; gsutil cat gs://dca-repl-airflow-dag/files/GCP_Archive_SourceFile.sh  | sh -s " + LANDING_HDFS_PATH + " " + ARCHIVE_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_UPSERT + " " + SRCRCVDT

repl_clust_fcst_wk_conv_upsert_file_archive_in_hdfs = BashOperator(
    task_id="repl_clust_fcst_wk_conv_upsert_file_archive_in_hdfs",
    bash_command=archive_command_upsert,
    dag=repl_clust_fcst_wk_conv_load_dag,
    trigger_rule='all_success'
)

# **************** Delete Oldest file from NFS mount Task started ******************

repl_clust_fcst_wk_conv_delete_oldest_file_delete = SSHOperator(
    task_id="repl_clust_fcst_wk_conv_delete_oldest_file_delete",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  '" + OLDEST_FILE_DELETE + "';",
    dag=repl_clust_fcst_wk_conv_load_dag
)

repl_clust_fcst_wk_conv_delete_oldest_file_upsert = SSHOperator(
    task_id="repl_clust_fcst_wk_conv_delete_oldest_file_upsert",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  '" + OLDEST_FILE_UPSERT + "';",
    dag=repl_clust_fcst_wk_conv_load_dag
)

# **************** Delete Signal file from NFS mount Task started ******************
repl_adj_sales_delete_signal_file = SSHOperator(
    task_id="delete_signal_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  " + SRC_SIGNAL_FILE_FULL_NAME + ";",
    on_success_callback=success_email_function,
    dag=repl_clust_fcst_wk_conv_load_dag
)

repl_clust_fcst_wk_conv_common_event_poll >> repl_clust_fcst_wk_conv_oldest_file_check >> assign_variable >> repl_clust_fcst_wk_conv_insert_file_move_to_hdfs >> repl_clust_fcst_wk_conv_update_file_move_to_hdfs >> zero_byte_file_check >> branch_task >> create_bfdms_dpaas_cluster >> repl_clust_fcst_wk_conv_spark_load_job >> delete_cluster
branch_task >> repl_clust_fcst_wk_conv_delete_file_archive_in_hdfs >> repl_clust_fcst_wk_conv_upsert_file_archive_in_hdfs >> repl_clust_fcst_wk_conv_delete_oldest_file_delete >> repl_clust_fcst_wk_conv_delete_oldest_file_upsert >> repl_adj_sales_delete_signal_file
