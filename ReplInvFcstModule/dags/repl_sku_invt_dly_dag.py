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
from depflow.operators.external_dependency_operator import ExternalDependencySensor

# sshHook = SSHHook('prod17-ssh-conn')
edgenode_sshHook = SSHHook('edgenode-ssh-conn')

SERVICE_ACCOUNT = Variable.get("service_account")
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
AF_INSTANCE_NAME = Variable.get("common_variables", deserialize_json=True)["af_instance_name"]

SUCCESS_MSG = "REPL_SKU_INVT_DLY table load was successful"

RUNMODE = "global"

CLUSTER_NAME = "repl-dl-ephe-repl-sku-invt-dly"

REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_sku_invt_dly"
DAG_DESC = "REPL SKU invt daily spark load job"
APP_NAME = "REPL_SKU_INVT_DLY"

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
            "num_instances": 9,
            "machine_type_uri": "n1-highmem-32",
            "disk_config": {
                "boot_disk_size_gb": 1000
            }
        },
        "secondary_worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-32",
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

repl_sku_invt_dly_load_dag = DAG(
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

FILE_PATH = "/edge_data/code/svcmdsedat/pricing/resources/outbound/prcg_sku_cost_rtl_dly"
SIGNAL_FILE_PATTERN = "prcg_sku_cost_rtl_dly_complete_*.trg"
POLL_PERIOD = "5"
WAIT_PERIOD = "120"

repl_sku_invt_dly_common_event_poll = SSHOperator(
    task_id="edge_node_common_event_file_poll",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/users/svcmdsedat/repl_sku_invt_dly/edgenode_file_poll_check.sh  " + FILE_PATH + " '" + SIGNAL_FILE_PATTERN + "' " + POLL_PERIOD + " " + WAIT_PERIOD,
    dag=repl_sku_invt_dly_load_dag
)

# ******************* Spark Job task started **********************

SCHEMA_NAME = Variable.get("repl_sku_invt_dly_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_sku_invt_dly_load_dag_var", deserialize_json=True)["table_name"]
# SCHEMA_NAME = "ww_repl_dl_secure"
# TABLE_NAME = "repl_sku_invt_dly"

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=repl_sku_invt_dly_load_dag,
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
                "spark.sql.shuffle.partitions": "800",
                "spark.executor.memory": "20g",
                "spark.driver.memory": "20g",
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
                     "runDQ=true", "enableservices=Dataquality,Audit", "env=" + ENV, "NumPartns=100",
                     "TgtTable=" + SCHEMA_NAME + "." + TABLE_NAME],

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml"
                          ]
        }
    }


repl_sku_invt_dly_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_sku_invt_dly_spark_load_job', job=create_spark_job("ReplSkuInvtDly"),
    gcp_conn_id=CONN_ID, dag=repl_sku_invt_dly_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_sku_invt_dly_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

DELETE_COMMAND = "rm  " + FILE_PATH + "/" + SIGNAL_FILE_PATTERN + ";"

repl_sku_invt_dly_del_trig_file = SSHOperator(
    task_id="Deleting_trigger_file",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command=DELETE_COMMAND,
    dag=repl_sku_invt_dly_load_dag
)


repl_sku_invt_dly_dep_sensor1 = ExternalDependencySensor(task_id="repl_sku_invt_dly_dep_sensor1", sock_conn_id="default_depflow",
                                       ref_dag_id="repl_sku_in_stk_dly_dag", ref_task_id="delete_cluster",
                                       ref_instance_name=AF_INSTANCE_NAME,
                                       poke_interval=30,
                                       mode="reschedule", delta="24h", allowed_statuses=["success"],
                                       method="before_task", gcp_conn_id=CONN_ID, dag=repl_sku_invt_dly_load_dag
                                       )


repl_sku_invt_dly_dep_sensor2 = ExternalDependencySensor(task_id="repl_sku_invt_dly_dep_sensor2", sock_conn_id="default_depflow",
                                       ref_dag_id="repl_managed_sku_dag", ref_task_id="delete_cluster",
                                       ref_instance_name=AF_INSTANCE_NAME,
                                       poke_interval=30,
                                       mode="reschedule", delta="24h", allowed_statuses=["success"],
                                       method="before_task", gcp_conn_id=CONN_ID, dag=repl_sku_invt_dly_load_dag
                                       )

repl_sku_invt_dly_common_event_poll >> create_bfdms_dpaas_cluster >> repl_sku_invt_dly_spark_load_job >> delete_cluster >> repl_sku_invt_dly_del_trig_file
