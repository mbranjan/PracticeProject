from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.utils.email import send_email
from bfdms.dpaas import BFDMSDataprocCreateClusterOperator
from bfdms.dpaas import BFDMSDataprocSubmitJobOperator as DataprocSubmitJobOperator
from bfdms.dpaas import BFDMSDataprocDeleteClusterOperator as DataprocDeleteClusterOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import base64
from bfdms.dpaas import GetDefaultRegionOperator
from dca.operators.job import DCASubmitJobOperator
from dca.operators.clusters import DCACreateClusterOperator
import os
import pytz

cst_tz = pytz.timezone("America/Chicago")
sshHook = SSHHook('prod17-ssh-conn')
edgenode_sshHook = SSHHook('edgenode-ssh-conn')

CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
SUCCESS_MSG = "repl_rs_clust_store_assgn table load was successful"

CLUSTER_NAME = "repl-dl-ephe-repl-rs-clust-store-assgn"
ARTIFACTS_BUCKET = "gs://dca-repl-airflow-dag"
FILE_NAME = str(os.path.basename(__file__).split(".")[0])[4:]

dr_dpaas_config = eval(Variable.get("dr_dpaas_config"))
REGION = GetDefaultRegionOperator.get_region(dr_dpaas_config)

DAG_NAME = "dag-" + FILE_NAME
DAG_DESC = "repl_rs_clust_store_assgn spark load job"
APP_NAME = "repl_rs_clust_store_assgn"


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

repl_rs_clust_store_assgn_load_dag = DAG(
    DAG_NAME,
    description=DAG_DESC,
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 8, 1, tzinfo=cst_tz),
    max_active_runs=1,
    concurrency=5,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['ReplFullLoadModule']
)

# **************** File poll check Task started ******************
FILE_PATH = "/grid_transient/EIMDI"
SIGNAL_FILE_PATTERN = "Signal_RC_CLUSTER_STORE_ASSGN_*_JWUSRG2U.txt"
POLL_PERIOD = "5"
WAIT_PERIOD = "240"

repl_rs_clust_store_assgn_common_event_poll = SSHOperator(
    task_id="common_event_poll",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/Aorta_file_poll_check.sh  " + FILE_PATH + " " + SIGNAL_FILE_PATTERN + " " + POLL_PERIOD + " " + WAIT_PERIOD,
    dag=repl_rs_clust_store_assgn_load_dag,
    trigger_rule='all_success'
)

# **************** Oldest File check Task started ******************
repl_rs_clust_store_assgn_oldest_file_check = SSHOperator(
    task_id="oldest_file_check",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/Aorta_Get_Oldest_Bin_File_Name_Date_Time.sh  " + FILE_PATH + " " + SIGNAL_FILE_PATTERN,
    dag=repl_rs_clust_store_assgn_load_dag,
    trigger_rule='all_success'
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
    dag=repl_rs_clust_store_assgn_load_dag,
    trigger_rule='all_success'
)

# **************** File move to HDFS Task started ******************
SRC_SIGNAL_FILE_FULL_NAME = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRC_SIGNAL_FILE_FULL_NAME') }}"
DATETIME = "{{ ti.xcom_pull(task_ids='assign_variable', key='DATETIME') }}"
OLDEST_FILE = FILE_PATH + "/RC_CLUSTER_STORE_ASSGN_" + DATETIME + "_JWUSRG2U.txt"
LANDING_HDFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/landing"
ARCHIVE_HDFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/raw"
LOAD_TYPE = "incremental"
COUNTRY_CODE = "us"
SCHEMA_NAME = Variable.get("repl_rs_clust_store_assgn_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_rs_clust_store_assgn_load_dag_var", deserialize_json=True)["table_name"]
# SCHEMA_NAME = "ww_repl_dl_secure"
# TABLE_NAME = "repl_rs_clust_store_assgn"
# SRCRCVDT = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVDT') }}"
# SRCRCVTS = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVTS') }}"
SRCRCVTS = datetime.now(pytz.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
SRCRCVDT = str(date.today())

LAKE_BUCKET = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd"
GCP_LOCATION = LAKE_BUCKET + "/user/svcmdsedat/landing/incremental/us/" + SCHEMA_NAME + "/" + TABLE_NAME + "/src_rcv_dt=" + SRCRCVDT
GEO_RGN_CD = "US"

# sh /u/appaorta/aorta/scripts/nfs_cp_resources/Bin_File_GCS_Put.sh &OLDEST_FILE# &LANDING_HDFS_PATH# &LOAD_TYPE# &COUNTRY_CODE# &SCHEMA_NAME# &TABLE_NAME# &SRCRCVDT#
repl_rs_clust_store_assgn_file_move_to_hdfs = SSHOperator(
    task_id="file_move_to_hdfs",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/nfs_cp_resources/Bin_File_GCS_Put.sh  '" + OLDEST_FILE + "' " + LANDING_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_NAME + " " + SRCRCVDT,
    dag=repl_rs_clust_store_assgn_load_dag,
    trigger_rule='all_success'
)

# ******************* Spark Job task started **********************

cluster_configs = {
    "config": f"{ARTIFACTS_BUCKET}/cluster_config/{FILE_NAME}-cluster-config.json",
    "common_config": f"{ARTIFACTS_BUCKET}/cluster_config/common-cluster-config-mdsereplprd.json"
}

spark_config = {
    "config": f"{ARTIFACTS_BUCKET}/spark_config/{FILE_NAME}-spark-config.json",
    "common_config": f"{ARTIFACTS_BUCKET}/spark_config/common-spark-config.json"
}

args = ["location=" + GCP_LOCATION, "src_rcv_dt=" + SRCRCVDT, "schema=" + SCHEMA_NAME, "table=" + TABLE_NAME,
        "src_rcv_ts=" + SRCRCVTS]

create_bfdms_dpaas_cluster = DCACreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME,
    location=REGION,
    gcp_conn_id=CONN_ID,
    configs=cluster_configs,
    dag=repl_rs_clust_store_assgn_load_dag,
    trigger_rule='all_success',
    env=ENV)

repl_rs_clust_store_assgn_spark_load_job = DCASubmitJobOperator(
    task_id="repl_rs_clust_store_assgn_spark_load_job",
    dag=repl_rs_clust_store_assgn_load_dag,
    region=REGION,
    cluster_name=CLUSTER_NAME, gcp_conn_id=CONN_ID,
    configs=spark_config, env=ENV, sparkargs=args,
    trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_rs_clust_store_assgn_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

# **************** HDFS File archive Task started ******************
# command_2 = "gcloud auth activate-service-account --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; gsutil cat gs://dca-repl-airflow-dag/files/GCP_Archive_SourceFile.sh  | sh -s " + LANDING_HDFS_PATH + " " + ARCHIVE_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_NAME + " " + SRCRCVDT
#
# repl_rs_clust_store_assgn_file_archive_in_hdfs = BashOperator(
#     task_id="repl_rs_clust_store_assgn_file_archive_in_hdfs",
#     bash_command=command_2,
#     dag=repl_rs_clust_store_assgn_load_dag,
#     trigger_rule='all_success'
# )

repl_rs_clust_store_assgn_file_archive_in_hdfs = SSHOperator(
    task_id="repl_rs_clust_store_assgn_file_archive_in_hdfs",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/scripts/GCP_Archive_SourceFile.sh  " + LANDING_HDFS_PATH + " " + ARCHIVE_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_NAME + " " + SRCRCVDT,
    dag=repl_rs_clust_store_assgn_load_dag,
    trigger_rule='all_success'
)

# **************** Delete Oldest file from NFS mount Task started ******************

repl_rs_clust_store_assgn_delete_oldest_file = SSHOperator(
    task_id="delete_oldest_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  '" + OLDEST_FILE + "';",
    dag=repl_rs_clust_store_assgn_load_dag,
    trigger_rule='all_success'
)

# **************** Delete Signal file from NFS mount Task started ******************
repl_rs_clust_store_assgn_delete_signal_file = SSHOperator(
    task_id="delete_signal_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  " + SRC_SIGNAL_FILE_FULL_NAME + ";",
    dag=repl_rs_clust_store_assgn_load_dag,
    trigger_rule='all_success',
    on_success_callback=success_email_function
)

repl_rs_clust_store_assgn_common_event_poll >> repl_rs_clust_store_assgn_oldest_file_check >> assign_variable >> repl_rs_clust_store_assgn_file_move_to_hdfs >> create_bfdms_dpaas_cluster >> repl_rs_clust_store_assgn_spark_load_job >> repl_rs_clust_store_assgn_file_archive_in_hdfs >> repl_rs_clust_store_assgn_delete_oldest_file >> repl_rs_clust_store_assgn_delete_signal_file
repl_rs_clust_store_assgn_spark_load_job >> delete_cluster

