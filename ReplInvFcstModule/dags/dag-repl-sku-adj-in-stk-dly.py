from datetime import date, timedelta
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
from bfdms.dpaas import GetDefaultRegionOperator
import os
from dca.operators.job import DCASubmitJobOperator

from dca.operators.clusters import DCACreateClusterOperator

edgenode_sshHook = SSHHook('edgenode-ssh-conn')

CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]

ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
SUCCESS_MSG = "repl_sku_adj_in_stk_dly table load was successful"

CLUSTER_NAME = "repl-dl-ephe-repl-sku-adj-in-stk-dly"

ARTIFACTS_BUCKET = "gs://dca-repl-airflow-dag"
FILE_NAME = str(os.path.basename(__file__).split(".")[0])[4:]

dr_dpaas_config = eval(Variable.get("dr_dpaas_config"))
REGION = GetDefaultRegionOperator.get_region(dr_dpaas_config)

DAG_NAME = "dag-" + FILE_NAME
DAG_DESC = "Repl SKU adjusted instock daily spark load job"
APP_NAME = "repl_sku_adj_in_stk_dly"


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

repl_sku_adj_in_stk_dly_load_dag = DAG(
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

# ******************* Spark Job tasks started **********************

SCHEMA_NAME = Variable.get("repl_sku_adj_in_stk_dly_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_sku_adj_in_stk_dly_load_dag_var", deserialize_json=True)["table_name"]
# SCHEMA_NAME = "ww_repl_dl_secure"
# TABLE_NAME = "repl_sku_adj_in_stk_dly"


LANDING_BUCKET = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/raw/incremental/us"
LANDING_PATH = LANDING_BUCKET + "/" + SCHEMA_NAME + "/" + TABLE_NAME

cluster_configs = {
    "config": f"{ARTIFACTS_BUCKET}/cluster_config/{FILE_NAME}-cluster-config.json",
    "common_config": f"{ARTIFACTS_BUCKET}/cluster_config/common-cluster-config-mdsereplprd.json"
}

spark_config1 = {
    "config": f"{ARTIFACTS_BUCKET}/spark_config/{FILE_NAME}-stage-load-spark-config.json",
    "common_config": f"{ARTIFACTS_BUCKET}/spark_config/common-spark-config.json"
}

spark_config2 = {
    "config": f"{ARTIFACTS_BUCKET}/spark_config/{FILE_NAME}-target-load-spark-config.json",
    "common_config": f"{ARTIFACTS_BUCKET}/spark_config/common-spark-config.json"
}

STAGE_LOAD_ARGS = ["Schema=" + SCHEMA_NAME, "Table=" + TABLE_NAME, "stgTable=" + TABLE_NAME, "LndngPth=" + LANDING_PATH,
                   "TgtTable=" + SCHEMA_NAME + "." + TABLE_NAME, "CatlgTable=" + SCHEMA_NAME + "." + TABLE_NAME]

TARGET_LOAD_ARGS = ["Schema=" + SCHEMA_NAME, "Table=" + TABLE_NAME, "stgTable=" + TABLE_NAME,
                    "LndngPth=" + LANDING_PATH, "TgtTable=" + SCHEMA_NAME + "." + TABLE_NAME,
                    "CatlgTable=" + SCHEMA_NAME + "." + TABLE_NAME]

create_bfdms_dpaas_cluster = DCACreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME,
    location=REGION,
    gcp_conn_id=CONN_ID,
    configs=cluster_configs,
    dag=repl_sku_adj_in_stk_dly_load_dag,
    trigger_rule='all_success',
    env=ENV)

repl_sku_adj_in_stk_dly_spark_stage_load_job = DCASubmitJobOperator(
    task_id="repl_sku_adj_in_stk_dly_spark_stage_load_job",
    dag=repl_sku_adj_in_stk_dly_load_dag,
    region=REGION,
    cluster_name=CLUSTER_NAME, gcp_conn_id=CONN_ID,
    configs=spark_config1, env=ENV, sparkargs=STAGE_LOAD_ARGS,
    trigger_rule='all_success')

repl_sku_adj_in_stk_dly_spark_target_load_job = DCASubmitJobOperator(
    task_id="repl_sku_adj_in_stk_dly_spark_target_load_job",
    dag=repl_sku_adj_in_stk_dly_load_dag,
    region=REGION,
    cluster_name=CLUSTER_NAME, gcp_conn_id=CONN_ID,
    configs=spark_config2, env=ENV, sparkargs=TARGET_LOAD_ARGS,
    trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_sku_adj_in_stk_dly_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

TOUCH_DATE = date.today().strftime("%Y-%m-%d%H%M%S")
TOUCH_FILE = "gs://repl-prod-handshake-files/" + TABLE_NAME + "/DONE@" + TOUCH_DATE
create_touch_file_command = "gcloud auth activate-service-account --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; export TZ='America/Chicago'; gsutil cp /dev/null " + TOUCH_FILE + " | sh -s "

repl_sku_adj_in_stk_dly_create_touch_file = BashOperator(
    task_id="repl_sku_adj_in_stk_dly_create_touch_file",
    bash_command=create_touch_file_command,
    dag=repl_sku_adj_in_stk_dly_load_dag,
    trigger_rule='all_success'
)

create_bfdms_dpaas_cluster >> repl_sku_adj_in_stk_dly_spark_stage_load_job >> repl_sku_adj_in_stk_dly_spark_target_load_job >> delete_cluster
repl_sku_adj_in_stk_dly_spark_target_load_job >> repl_sku_adj_in_stk_dly_create_touch_file
