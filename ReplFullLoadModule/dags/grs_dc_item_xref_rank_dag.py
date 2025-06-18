from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.email import send_email
from bfdms.dpaas import BFDMSDataprocCreateClusterOperator
from bfdms.dpaas import BFDMSDataprocSubmitJobOperator as DataprocSubmitJobOperator
from bfdms.dpaas import BFDMSDataprocDeleteClusterOperator as DataprocDeleteClusterOperator
from bfdms.dpaas import GetDefaultRegionOperator

from airflow.utils.dates import days_ago
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator
from airflow.models import Variable
import os
from dca.operators.job import DCASubmitJobOperator
import json
from dca.operators.clusters import DCACreateClusterOperator
from airflow.operators.python_operator import PythonOperator

dpaas_config = {
    'bfd_config': {
        'lifecycle': 'prod'
    }
}

REGION = GetDefaultRegionOperator(dpaas_config)
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]

CLUSTER_NAME = "repl-grs-dc-item-xref-rank"
# REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
ARTIFACTS_BUCKET = "gs://dca-repl-airflow-dag"
FILE_NAME = os.path.basename(__file__).split(".")[0]
# REGION = 'us-central1'

DAG_NAME = FILE_NAME
DAG_DESC = "GRS_DC_ITEM_XREF_RANK table load job"
APP_NAME = "GRS_DC_ITEM_XREF_RANK"

# PARENT_PROJECT = "wmt-dca-repl-dl-prod"
# PROJECT = "wmt-dca-repl-dl-prod"
# DATASET = "US_WM_REPL_VM"
# MATERIALIZATION_DATASET = "US_REPL_BQ_STG"

TGT_TBL = Variable.get("grs_dc_item_xref_rank_dag_var", deserialize_json=True)["tgt_tbl"]
# TGT_TBL = "us_wm_safety_stk_app_tables.grs_dc_item_xref_rank"
# TGT_TBL = "stg_ww_repl_dl_secure.grs_dc_item_xref_rank_test"

cluster_configs = {
    "config": f"{ARTIFACTS_BUCKET}/cluster_config/{FILE_NAME}.json",
    "common_config": f"{ARTIFACTS_BUCKET}/cluster_config/common_config_mdsereplprd.json"
}

spark_config = {
    "config": f"{ARTIFACTS_BUCKET}/spark_config/{FILE_NAME}.json",
    "common_config": f"{ARTIFACTS_BUCKET}/spark_config/common_spark_config.json"
}

args = [f"TgtTbl={TGT_TBL}"]


# REGION = GetDefaultRegionOperator(cluster_configs)


def success_email_function(context):
    dag_run = context.get('dag_run')
    msg = "Workflow has been completed successfully"
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

grs_dc_item_xref_rank_load_dag = DAG(
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

create_bfdms_dpaas_cluster = DCACreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME,
    location=REGION,
    gcp_conn_id=CONN_ID,
    configs=cluster_configs,
    dag=grs_dc_item_xref_rank_load_dag,
    trigger_rule='all_success',
    env=ENV)

grs_dc_item_xref_rank_spark_load_job = DCASubmitJobOperator(
    task_id="grs_dc_item_xref_rank_spark_load_job",
    dag=grs_dc_item_xref_rank_load_dag,
    region=REGION,
    cluster_name=CLUSTER_NAME, gcp_conn_id=CONN_ID,
    configs=spark_config, env=ENV, sparkargs=args,
    on_success_callback=success_email_function,
    trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=grs_dc_item_xref_rank_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

create_bfdms_dpaas_cluster >> grs_dc_item_xref_rank_spark_load_job >> delete_cluster
