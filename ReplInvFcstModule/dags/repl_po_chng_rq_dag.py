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

# sshHook = SSHHook('prod17-ssh-conn')
edgenode_sshHook = SSHHook('edgenode-ssh-conn')

SERVICE_ACCOUNT = Variable.get("service_account")
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
SUCCESS_MSG = "repl_po_chng_rq table load was successful"

RUNMODE = "global"

CLUSTER_NAME = "repl-dl-ephe-repl-po-chng-rq"

REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_po_chng_rq_dag"
DAG_DESC = "Repl po change request spark load job"
APP_NAME = "repl_po_chng_rq"

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
            "num_instances": 2,
            "machine_type_uri": "e2-standard-32",
            "disk_config": {
                "boot_disk_size_gb": 300
            }
        },
        "secondary_worker_config": {
            "num_instances": 1,
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

repl_po_chng_rq_load_dag = DAG(
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

# FILE_PATH = "/grid_transient/EIMDI"
SRC_FILE_NAME = "MF-LMNX.MNTMDSEB.N01.MDS.OMUS920.*"
NFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/LUMINEX/AVAILABILITY/REPL-PO-CHNG-RQ"
POLL_PERIOD = "2"
MAX_PERIOD = "240"

# sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh &NFS_PATH# &SRC_FILE_NAME# &POLL_PERIOD# &MAX_PERIOD#

repl_po_chng_rq_gcp_bucket_file_poll = SSHOperator(
    task_id="common_event_poll",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh  " + NFS_PATH + " " + SRC_FILE_NAME + " " + POLL_PERIOD + " " + MAX_PERIOD,
    dag=repl_po_chng_rq_load_dag
)

# **************** Oldest File check Task started ******************

# sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_get_oldest_bin_file_lmnx.sh &NFS_PATH# &SRC_FILE_NAME#

repl_po_chng_rq_oldest_file_check = SSHOperator(
    task_id="gcp_bucket_oldest_file_check",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_get_oldest_bin_file_lmnx.sh  " + NFS_PATH + " " + SRC_FILE_NAME,
    dag=repl_po_chng_rq_load_dag
)


def decode_and_assign_variable(ti):
    command_output = str(ti.xcom_pull(task_ids='gcp_bucket_oldest_file_check'))

    OLDEST_FILE = command_output.split("^")[0]
    SRCRCVTS = command_output.split("^")[1]
    SRCRCVDT = command_output.split("^")[2]

    ti.xcom_push(key="OLDEST_FILE", value=OLDEST_FILE)
    ti.xcom_push(key="SRCRCVTS", value=SRCRCVTS)
    ti.xcom_push(key="SRCRCVDT", value=SRCRCVDT)

    print("########### Variables inside decode_variable ############")
    print(OLDEST_FILE)
    print(SRCRCVTS)
    print(SRCRCVDT)


assign_variable = PythonOperator(
    task_id='assign_variable',
    python_callable=decode_and_assign_variable,
    provide_context=True,
    dag=repl_po_chng_rq_load_dag

)

# **************** File move to HDFS Task started ******************
OLDEST_FILE = "{{ ti.xcom_pull(task_ids='assign_variable', key='OLDEST_FILE') }}"
SRCRCVDT = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVDT') }}"
SRCRCVTS = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVTS') }}"
LANDING_BUCKET = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/landing"
RAW_BUCKET = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/raw"
LOAD_TYPE = "incremental"
COUNTRY_CODE = "us"
SCHEMA_NAME = Variable.get("repl_po_chng_rq_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_po_chng_rq_load_dag_var", deserialize_json=True)["table_name"]
# SCHEMA_NAME = "ww_mdse_dl_secure"
# TABLE_NAME = "repl_po_chng_rq"
TABLE_PATH = TABLE_NAME + "/upsert"

# sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_file_copy_lmnx.sh &OLDEST_FILE# &RAW_GCP_PATH# &LOAD_TYPE# &COUNTRY_CODE# &SCHEMA_NAME# &TABLE_NAME# &SRCRCVDT
repl_po_chng_rq_file_move_to_hdfs = SSHOperator(
    task_id="file_move_to_hdfs",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_file_copy_lmnx.sh  '" + OLDEST_FILE + "' " + LANDING_BUCKET + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH + " " + SRCRCVDT,
    dag=repl_po_chng_rq_load_dag,
    trigger_rule='all_success'
)

# AORTA_RIVER_HOME = '/edge_data/udp/data-pipeline-dpaas-lw-10.5'
# SQOOP_HOME = '/edge_data/dpaaslibs/bfdms-ien/dp1.5/lib/sqoop-1.4.7/'
# YAML_FILE = '/edge_data/code/svcmdsedat/availability/resources/yamls/repl_po_chng_rq/repl_po_chng_rq_external.yaml'
# LOAD_NAME = 'MdseSkuInvt'
# GCP_PROJECT = "wmt-bfdms-mdsedlprd"
# # CLUSTER_REGION = "us-central1"
# # CLUSTER_NAME = "repl-dl-ephe-mdse-sku-invt-adj"
#
# # gcloud config set project &GCP_PROJECT#
# # source ~/.profile
# # export SQOOP_HOME='/edge_data/dpaaslibs/bfdms-ien/dp1.5/lib/sqoop-1.4.7/'
# # export AORTA_RIVER_HOME='/edge_data/udp/data-pipeline-dpaas-lw-10.5'
# # export YAML_PATH='&YAML_PATH#'
# #
# # sh $AORTA_RIVER_HOME/run-data-pipeline.sh -yaml $YAML_PATH/&YAML_FILE# -connectionFile /edge_data/code/svcmdsedat/availability/resources/connections/connection.yaml -streamMode false -loadName MdseSkuInvt -env prod -runner local &key_value_params# -distribution DPAAS_NATIVE -dpaasGcpProject &GCP_PROJECT# -dpaasGcpProjectRegion &CLUSTER_REGION# -dpaasGcpCluster &CLUSTER_NAME#
#
# # ***************** Creating External Tables *********************
# repl_po_chng_rq_create_ext_table = SSHOperator(
#     task_id="create_external_table",
#     ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
#     command=f"sh {AORTA_RIVER_HOME}/run-data-pipeline.sh  -yaml {YAML_FILE} -connectionFile /edge_data/code/svcmdsedat/availability/resources/connections/connection.yaml -streamMode false -loadName {LOAD_NAME} -env {env} -runner local -kv Table={TABLE_NAME} -kv CopybookPath=gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/copybook/uswmt_repl_po_chng_req.cfd -kv landing_bucket=gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd -kv FilePath=gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/landing/incremental/us/ww_repl_dl_secure/repl_po_chng_rq/upsert/src_rcv_dt={SRCRCVDT} -distribution DPAAS_NATIVE -dpaasGcpProject {GCP_PROJECT} ",
#     dag=repl_po_chng_rq_dag
# )

# ******************* Spark Job task started **********************

OP_CMPNY_CD = "WMT-US"
GEO_RGN_CD = "US"
LOCATION = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/landing/incremental/us/" + SCHEMA_NAME + "/" + TABLE_NAME + "/upsert/src_rcv_dt"
create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=repl_po_chng_rq_load_dag,
    dpaas_config=dpaas_config,
    gcp_conn_id=CONN_ID,
    trigger_rule='all_success')


def create_spark_job(WORKFLOW_NAME):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Inv_Fcst_Module/ReplInvFcstModule-1.109.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.8-bundled.jar",
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
                "spark.executor.instances": "20",
                "spark.executor.cores": "4",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.shuffle.partitions": "400",
                "spark.executor.memory": "12g",
                "spark.driver.memory": "12g",
                "spark.executor.memoryOverhead": "3g",
                "spark.driver.memoryOverhead": "3g",
                "spark.yarn.maxAppAttempts": "1",
                "spark.driver.extraJavaOptions": "-Dudp.conn.file=connection.yaml -Dudp.job.file=archetype_spark.yaml"

            },

            "args": ["workflow=" + WORKFLOW_NAME, "runmode=" + RUNMODE, "BASEDIVNBR=1", "geo_region_cd=" + GEO_RGN_CD,
                     "op_cmpny_cd=" + OP_CMPNY_CD, "Schema=" + SCHEMA_NAME, "Table=" + TABLE_NAME,
                     "enableservices=DATAQUALITY,AUDIT", "runDQ=true", "env=" + ENV,
                     "TgtTable=" + SCHEMA_NAME + "." + TABLE_NAME, "CatlgTable=" + SCHEMA_NAME + "." + TABLE_NAME,
                     "extTable=stg_ww_repl_dl_secure.STG_US_WM_repl_sku_manl_order_EXT", "src_rcv_ts" + SRCRCVTS,
                     "src_rcv_dt=" + SRCRCVDT, "location=" + LOCATION, "NumPartns=10"],

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml"
                          ]
        }
    }


repl_po_chng_rq_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_po_chng_rq_spark_load_job', job=create_spark_job("ReplPoChngRqWF"),
    gcp_conn_id=CONN_ID, dag=repl_po_chng_rq_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_po_chng_rq_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

# **************** HDFS File archive Task started ******************

# sh /edge_data/code/svcmdsedat/availability/resources/scripts/GCP_Archive_SourceFile.sh &LANDING_BUCKET# &RAW_BUCKET# &LOAD_TYPE# &COUNTRY_CODE# &SCHEMA_NAME# &TABLE_NAME# &SRCRCVDT#
repl_po_chng_rq_file_archive_in_hdfs = SSHOperator(
    task_id="file_archive_in_hdfs",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/scripts/GCP_Archive_SourceFile.sh  " + LANDING_BUCKET + " " + RAW_BUCKET + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH + " " + SRCRCVDT,
    dag=repl_po_chng_rq_load_dag
)

repl_po_chng_rq_gcp_bucket_file_poll >> repl_po_chng_rq_oldest_file_check >> assign_variable >> repl_po_chng_rq_file_move_to_hdfs >> create_bfdms_dpaas_cluster >> repl_po_chng_rq_spark_load_job >> delete_cluster >> repl_po_chng_rq_file_archive_in_hdfs
