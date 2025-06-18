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
edgenode_sshHook = SSHHook('edgenode-ssh-conn')

SERVICE_ACCOUNT = Variable.get("service_account")
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
SUCCESS_MSG = "repl_sku_invt_wkly table load was successful"

RUNMODE = "global"

CLUSTER_NAME = "repl-dl-ephe-repl-sku-invt-wkly"

REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_sku_invt_wkly_dag"
DAG_DESC = "Repl SKU invt weekly spark load job"
APP_NAME = "repl_sku_invt_wkly"

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
                "boot_disk_size_gb": 1000
            }
        },
        "worker_config": {
            "num_instances": 12,
            "machine_type_uri": "n1-highmem-32",
            "disk_config": {
                "boot_disk_size_gb": 1000
            }
        },
        "secondary_worker_config": {
            "num_instances": 3,
            "machine_type_uri": "n1-highmem-32",
            "disk_config": {
                "boot_disk_size_gb": 1000
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

repl_sku_invt_wkly_load_dag = DAG(
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

NFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/LUMINEX/AVAILABILITY/REPL-SKU-INVT-WKLY-S1"
POLL_PERIOD = "5"
MAX_PERIOD = "240"
SRC_FILE_NAME_681 = "MF-LMNX.MNTGRDB.N01.MDS.TD681.EXT.BKP.*"
SRC_FILE_NAME_682 = "MF-LMNX.MNTGRDB.N01.MDS.TD682.EXT.BKP.*"
SRC_FILE_NAME_683 = "MF-LMNX.MNTGRDB.N01.MDS.TD683.EXT.BKP.*"
SRC_FILE_NAME_684 = "MF-LMNX.MNTGRDB.N01.MDS.TD684.EXT.BKP.*"
SRC_FILE_NAME_685 = "MF-LMNX.MNTGRDB.N01.MDS.TD685.EXT.BKP.*"

# sh  /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh &NFS_PATH# &SRC_FILE_NAME# &POLL_PERIOD# &MAX_PERIOD#

repl_sku_invt_wkly_gcp_bucket_file_poll_681 = SSHOperator(
    task_id="common_event_poll_file_681",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh  /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh  " + NFS_PATH + " " + SRC_FILE_NAME_681 + " " + POLL_PERIOD + " " + MAX_PERIOD,
    dag=repl_sku_invt_wkly_load_dag
)

repl_sku_invt_wkly_gcp_bucket_file_poll_682 = SSHOperator(
    task_id="common_event_poll_file_682",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh  /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh  " + NFS_PATH + " " + SRC_FILE_NAME_682 + " " + POLL_PERIOD + " " + MAX_PERIOD,
    dag=repl_sku_invt_wkly_load_dag
)

repl_sku_invt_wkly_gcp_bucket_file_poll_683 = SSHOperator(
    task_id="common_event_poll_file_683",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh  /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh  " + NFS_PATH + " " + SRC_FILE_NAME_683 + " " + POLL_PERIOD + " " + MAX_PERIOD,
    dag=repl_sku_invt_wkly_load_dag
)

repl_sku_invt_wkly_gcp_bucket_file_poll_684 = SSHOperator(
    task_id="common_event_poll_file_684",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh  /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh  " + NFS_PATH + " " + SRC_FILE_NAME_684 + " " + POLL_PERIOD + " " + MAX_PERIOD,
    dag=repl_sku_invt_wkly_load_dag
)

repl_sku_invt_wkly_gcp_bucket_file_poll_685 = SSHOperator(
    task_id="common_event_poll_file_685",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh  /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_bucket_file_poll.sh  " + NFS_PATH + " " + SRC_FILE_NAME_685 + " " + POLL_PERIOD + " " + MAX_PERIOD,
    dag=repl_sku_invt_wkly_load_dag
)

# ************************* Oldest File check started For File_681  *************************

# sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_get_oldest_bin_file_lmnx.sh &NFS_PATH# &SRC_FILE_NAME#

repl_sku_invt_wkly_oldest_file_check_681 = SSHOperator(
    task_id="gcp_bucket_oldest_file_check_681",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_get_oldest_bin_file_lmnx.sh  " + NFS_PATH + " '" + SRC_FILE_NAME_681 + "'",
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)


def decode_and_assign_variable_681(ti):
    command_output = str(ti.xcom_pull(task_ids='gcp_bucket_oldest_file_check_681'))

    OLDEST_FILE_681 = command_output.split("^")[0]
    SRCRCVTS = command_output.split("^")[1]
    SRCRCVDT = command_output.split("^")[2]

    ti.xcom_push(key="OLDEST_FILE_681", value=OLDEST_FILE_681)
    ti.xcom_push(key="SRCRCVTS", value=SRCRCVTS)
    ti.xcom_push(key="SRCRCVDT", value=SRCRCVDT)

    print("########### Variables inside decode_variable ############")
    print(OLDEST_FILE_681)
    print(SRCRCVTS)
    print(SRCRCVDT)


assign_variable_681 = PythonOperator(
    task_id='assign_variable_681',
    python_callable=decode_and_assign_variable_681,
    provide_context=True,
    dag=repl_sku_invt_wkly_load_dag
)

# ************************* Oldest File check started For File_682  ******************

repl_sku_invt_wkly_oldest_file_check_682 = SSHOperator(
    task_id="gcp_bucket_oldest_file_check_682",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_get_oldest_bin_file_lmnx.sh  " + NFS_PATH + " '" + SRC_FILE_NAME_682 + "'",
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)


def decode_and_assign_variable_682(ti):
    command_output = str(ti.xcom_pull(task_ids='gcp_bucket_oldest_file_check_682'))
    OLDEST_FILE_682 = command_output.split("^")[0]
    ti.xcom_push(key="OLDEST_FILE_682", value=OLDEST_FILE_682)
    print("########### Variables inside decode_variable ############")
    print(OLDEST_FILE_682)


assign_variable_682 = PythonOperator(
    task_id='assign_variable_682',
    python_callable=decode_and_assign_variable_682,
    provide_context=True,
    dag=repl_sku_invt_wkly_load_dag
)

# ************************* Oldest File check started For File_683  ******************

repl_sku_invt_wkly_oldest_file_check_683 = SSHOperator(
    task_id="gcp_bucket_oldest_file_check_683",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_get_oldest_bin_file_lmnx.sh  " + NFS_PATH + " '" + SRC_FILE_NAME_683 + "'",
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)


def decode_and_assign_variable_683(ti):
    command_output = str(ti.xcom_pull(task_ids='gcp_bucket_oldest_file_check_683'))
    OLDEST_FILE_683 = command_output.split("^")[0]
    ti.xcom_push(key="OLDEST_FILE_683", value=OLDEST_FILE_683)
    print("########### Variables inside decode_variable ############")
    print(OLDEST_FILE_683)


assign_variable_683 = PythonOperator(
    task_id='assign_variable_683',
    python_callable=decode_and_assign_variable_683,
    provide_context=True,
    dag=repl_sku_invt_wkly_load_dag
)

# ************************* Oldest File check started For File_684  ******************

repl_sku_invt_wkly_oldest_file_check_684 = SSHOperator(
    task_id="gcp_bucket_oldest_file_check_684",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_get_oldest_bin_file_lmnx.sh  " + NFS_PATH + " '" + SRC_FILE_NAME_684 + "'",
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)


def decode_and_assign_variable_684(ti):
    command_output = str(ti.xcom_pull(task_ids='gcp_bucket_oldest_file_check_684'))
    OLDEST_FILE_684 = command_output.split("^")[0]
    ti.xcom_push(key="OLDEST_FILE_684", value=OLDEST_FILE_684)
    print("########### Variables inside decode_variable ############")
    print(OLDEST_FILE_684)


assign_variable_684 = PythonOperator(
    task_id='assign_variable_684',
    python_callable=decode_and_assign_variable_684,
    provide_context=True,
    dag=repl_sku_invt_wkly_load_dag
)

# ************************* Oldest File check started For File_685  ******************

repl_sku_invt_wkly_oldest_file_check_685 = SSHOperator(
    task_id="gcp_bucket_oldest_file_check_685",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_get_oldest_bin_file_lmnx.sh  " + NFS_PATH + " '" + SRC_FILE_NAME_685 + "'",
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)


def decode_and_assign_variable_685(ti):
    command_output = str(ti.xcom_pull(task_ids='gcp_bucket_oldest_file_check_685'))
    OLDEST_FILE_685 = command_output.split("^")[0]
    ti.xcom_push(key="OLDEST_FILE_685", value=OLDEST_FILE_685)
    print("########### Variables inside decode_variable ############")
    print(OLDEST_FILE_685)


assign_variable_685 = PythonOperator(
    task_id='assign_variable_685',
    python_callable=decode_and_assign_variable_685,
    provide_context=True,
    dag=repl_sku_invt_wkly_load_dag
)

# **************** File move to HDFS Task started ******************
OLDEST_FILE_681 = "{{ ti.xcom_pull(task_ids='assign_variable_681', key='OLDEST_FILE_681') }}"
OLDEST_FILE_682 = "{{ ti.xcom_pull(task_ids='assign_variable_682', key='OLDEST_FILE_682') }}"
OLDEST_FILE_683 = "{{ ti.xcom_pull(task_ids='assign_variable_683', key='OLDEST_FILE_683') }}"
OLDEST_FILE_684 = "{{ ti.xcom_pull(task_ids='assign_variable_684', key='OLDEST_FILE_684') }}"
OLDEST_FILE_685 = "{{ ti.xcom_pull(task_ids='assign_variable_685', key='OLDEST_FILE_685') }}"

SCHEMA_NAME = Variable.get("repl_sku_invt_wkly_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_sku_invt_wkly_load_dag_var", deserialize_json=True)["table_name"]
# SCHEMA_NAME = "ww_mdse_dl_secure"
# TABLE_NAME = "repl_sku_invt_wkly"

TABLE_PATH = TABLE_NAME + "/S1"
LANDING_BUCKET = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/raw"
LOAD_TYPE = "incremental"
COUNTRY_CODE = "us"

SRCRCVDT = "{{ ti.xcom_pull(task_ids='assign_variable_681', key='SRCRCVDT') }}"
SRCRCVTS = "{{ ti.xcom_pull(task_ids='assign_variable_681', key='SRCRCVTS') }}"

# sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_file_copy_lmnx.sh &OLDEST_FILE# &LANDING_BUCKET# &LOAD_TYPE# &COUNTRY_CODE# &SCHEMA_NAME# &TABLE_PATH# &SRCRCVDT#
repl_sku_invt_wkly_file_move_to_hdfs_681 = SSHOperator(
    task_id="repl_sku_invt_wkly_file_move_to_hdfs_681",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_file_copy_lmnx.sh  '" + OLDEST_FILE_681 + "' " + LANDING_BUCKET + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH + " " + SRCRCVDT,
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)

repl_sku_invt_wkly_file_move_to_hdfs_682 = SSHOperator(
    task_id="repl_sku_invt_wkly_file_move_to_hdfs_682",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_file_copy_lmnx.sh  '" + OLDEST_FILE_682 + "' " + LANDING_BUCKET + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH + " " + SRCRCVDT,
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)

repl_sku_invt_wkly_file_move_to_hdfs_683 = SSHOperator(
    task_id="repl_sku_invt_wkly_file_move_to_hdfs_683",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_file_copy_lmnx.sh  '" + OLDEST_FILE_683 + "' " + LANDING_BUCKET + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH + " " + SRCRCVDT,
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)

repl_sku_invt_wkly_file_move_to_hdfs_684 = SSHOperator(
    task_id="repl_sku_invt_wkly_file_move_to_hdfs_684",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_file_copy_lmnx.sh  '" + OLDEST_FILE_684 + "' " + LANDING_BUCKET + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH + " " + SRCRCVDT,
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)

repl_sku_invt_wkly_file_move_to_hdfs_685 = SSHOperator(
    task_id="repl_sku_invt_wkly_file_move_to_hdfs_685",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/yamls/repl/lmnx/gcp_file_copy_lmnx.sh  '" + OLDEST_FILE_685 + "' " + LANDING_BUCKET + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH + " " + SRCRCVDT,
    dag=repl_sku_invt_wkly_load_dag,
    trigger_rule='all_success'
)

# ******************* Spark Job task started **********************

OP_CMPNY_CD = "WMT-US"
GEO_RGN_CD = "US"

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=repl_sku_invt_wkly_load_dag,
    dpaas_config=dpaas_config,
    gcp_conn_id=CONN_ID,
    trigger_rule='all_success')


def create_spark_job(WORKFLOW_NAME):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Inv_Fcst_Module/ReplInvFcstModule-1.97.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.8-bundled.jar",
                              REPL_JAR_PATH + "hive-serde-ebcdic2ascii-0.0.1-SNAPSHOT-jar-with-dependencies.jar",
                              REPL_JAR_PATH + "hive-contrib-2.3.7.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar",
                              REPL_JAR_PATH + "mail-1.4.7.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkflowController",
            "properties": {
                "spark.sql.storeAssignmentPolicy": "legacy",
                "spark.sql.legacy.charVarcharAsString": "true",
                "spark.submit.deployMode": "cluster",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.instances": "150",
                "spark.executor.cores": "3",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.shuffle.partitions": "600",
                "spark.executor.memory": "20g",
                "spark.driver.memory": "20g",
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

            "args": ["workflow=" + WORKFLOW_NAME, "runmode=" + RUNMODE, "Schema=" + SCHEMA_NAME, "Table=" + TABLE_NAME,
                     "CatlgTable=" + SCHEMA_NAME + "." + TABLE_NAME, "TgtTable=" + SCHEMA_NAME + "." + TABLE_NAME,
                     "extTable=stg_ww_repl_dl_secure.stg_REPL_SKU_INVT_WKLY_ext_s1",
                     "DqTable=stg_ww_repl_dl_secure.stg_REPL_SKU_INVT_WKLY_dq", "enableservices=Dataquality,Audit",
                     "env=" + ENV, "runDQ=true", "NumPartns=500", "geo_region_cd=" + GEO_RGN_CD,
                     "op_cmpny_cd=" + OP_CMPNY_CD, "src_rcv_ts=" + SRCRCVTS, "SRCRCVDT=" + SRCRCVDT],

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml"
                          ]
        }
    }


repl_sku_invt_wkly_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_invt_wkly_spark_load_job', job=create_spark_job("REPL_SKU_INVT_WKLY"),
    gcp_conn_id=CONN_ID, dag=repl_sku_invt_wkly_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_sku_invt_wkly_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

# ******************* Creating Trig File **********************

trig_command = "touch  /grid_transient/EIMDI/" + TABLE_NAME + "_distcp_gcs_prod17_000.trig" + ";"
repl_sku_invt_wkly_create_trig_file = SSHOperator(
    task_id="repl_sku_invt_wkly_create_trig_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command=trig_command,
    dag=repl_sku_invt_wkly_load_dag
)

repl_sku_invt_wkly_gcp_bucket_file_poll_681 >> repl_sku_invt_wkly_oldest_file_check_681 >> assign_variable_681 >> repl_sku_invt_wkly_file_move_to_hdfs_681 >> create_bfdms_dpaas_cluster
repl_sku_invt_wkly_gcp_bucket_file_poll_682 >> repl_sku_invt_wkly_oldest_file_check_682 >> assign_variable_682 >> repl_sku_invt_wkly_file_move_to_hdfs_682 >> create_bfdms_dpaas_cluster
repl_sku_invt_wkly_gcp_bucket_file_poll_683 >> repl_sku_invt_wkly_oldest_file_check_683 >> assign_variable_683 >> repl_sku_invt_wkly_file_move_to_hdfs_683 >> create_bfdms_dpaas_cluster
repl_sku_invt_wkly_gcp_bucket_file_poll_684 >> repl_sku_invt_wkly_oldest_file_check_684 >> assign_variable_684 >> repl_sku_invt_wkly_file_move_to_hdfs_684 >> create_bfdms_dpaas_cluster
repl_sku_invt_wkly_gcp_bucket_file_poll_685 >> repl_sku_invt_wkly_oldest_file_check_685 >> assign_variable_685 >> repl_sku_invt_wkly_file_move_to_hdfs_685 >> create_bfdms_dpaas_cluster
create_bfdms_dpaas_cluster >> repl_sku_invt_wkly_spark_load_job >> delete_cluster >> repl_sku_invt_wkly_create_trig_file
