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
import pytz

cst_tz = pytz.timezone("America/Chicago")
sshHook = SSHHook('prod17-ssh-conn')
edgenode_sshHook = SSHHook('edgenode-ssh-conn')

SERVICE_ACCOUNT = Variable.get("service_account")
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]

SUCCESS_MSG = "repl_dmdunit_dly table load was successful"

CLUSTER_NAME = "repl-dl-ephe-repl-dmdunit-dly"
REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "repl_dmdunit_dly_dag"
DAG_DESC = "REPL DMDUNIT DLY spark load job"
APP_NAME = "REPL_DMDUNIT_DLY"

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
                "boot_disk_size_gb": 400
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

repl_dmdunit_dly_load_dag = DAG(
    DAG_NAME,
    description=DAG_DESC,
    default_args=default_args,
    schedule_interval='15 10 * * *',
    start_date=datetime(2024, 8, 1, tzinfo=cst_tz),
    max_active_runs=1,
    concurrency=5,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['ReplDmdUnitModule']

)

# **************** File poll check Task started ******************
FILE_PATH = "/grid_transient/EIMDI"
SIGNAL_FILE_PATTERN = "Signal_GRS_DMDUNIT_USWMTNIGHTBATCH*.txt"
POLL_PERIOD = "5"
WAIT_PERIOD = "120"

repl_dmdunit_dly_common_event_poll = SSHOperator(
    task_id="common_event_poll",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/Aorta_file_poll_check.sh  " + FILE_PATH + " " + SIGNAL_FILE_PATTERN + " " + POLL_PERIOD + " " + WAIT_PERIOD,
    dag=repl_dmdunit_dly_load_dag,
    trigger_rule='all_success'
)

# **************** Oldest File check Task started ******************
repl_dmdunit_dly_oldest_file_check = SSHOperator(
    task_id="oldest_file_check",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/Aorta_Get_Oldest_Bin_File_Name_Date_Time.sh  " + FILE_PATH + " " + SIGNAL_FILE_PATTERN,
    dag=repl_dmdunit_dly_load_dag,
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
    dag=repl_dmdunit_dly_load_dag,
    trigger_rule='all_success'
)

# **************** File move to HDFS Task started ******************
SRC_SIGNAL_FILE_FULL_NAME = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRC_SIGNAL_FILE_FULL_NAME') }}"
DATETIME = "{{ ti.xcom_pull(task_ids='assign_variable', key='DATETIME') }}"
OLDEST_FILE = FILE_PATH + "/GRS_DMDUNIT_UPSERT_USWMTNIGHTBATCH" + DATETIME + "*"

LANDING_HDFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/landing"
ARCHIVE_HDFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/raw"
LOAD_TYPE = "incremental"
COUNTRY_CODE = "us"
SCHEMA_NAME = Variable.get("repl_dmdunit_dly_load_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("repl_dmdunit_dly_load_dag_var", deserialize_json=True)["table_name"]
# SCHEMA_NAME = "ww_repl_dl_secure"
# TABLE_NAME = "repl_dmdunit_dly"
SRCRCVDT = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVDT') }}"
SRCRCVTS = "{{ ti.xcom_pull(task_ids='assign_variable', key='SRCRCVTS') }}"

TABLE_PATH_UPSERT = TABLE_NAME + "/upsert"

TGT_TABLE = SCHEMA_NAME + "." + TABLE_NAME

# LAKE_BUCKET = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd"
GCP_LOCATION = LANDING_HDFS_PATH + "/incremental/us/" + SCHEMA_NAME + "/" + TABLE_PATH_UPSERT + "/src_rcv_dt"
GEO_RGN_CD = "US"
OP_CMPNY_CD = "WMT-US"

repl_dmdunit_dly_file_move_to_hdfs = SSHOperator(
    task_id="file_move_to_hdfs",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /u/appaorta/aorta/scripts/nfs_cp_resources/Bin_File_GCS_Put.sh  '" + OLDEST_FILE + "' " + LANDING_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_UPSERT + " " + SRCRCVDT,
    dag=repl_dmdunit_dly_load_dag,
    trigger_rule='all_success'
)

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=repl_dmdunit_dly_load_dag,
    dpaas_config=dpaas_config,
    gcp_conn_id=CONN_ID,
    trigger_rule='all_success')


def create_spark_job(WORKFLOW_NAME):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_Dmdunit_Module/ReplDmdUnitModule-1.113.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.8-bundled.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar",
                              REPL_JAR_PATH + "hive-serde-ebcdic2ascii-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkflowController",
            "properties": {
                "spark.driver.memory": "12g",
                "spark.executor.memory": "12g",
                "spark.executor.cores": "4",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.executor.instances": "50",
                "spark.submit.deployMode": "cluster",
                "spark.dynamicAllocation.enabled": "false",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.shuffle.partitions": "400",
                "spark.yarn.executor.memoryOverhead": "3g",
                "spark.yarn.driver.memoryOverhead": "3g",
                "spark.driver.extraJavaOptions": "-Dudp.conn.file=connection.yaml -Dudp.job.file=archetype_spark.yaml",
                "spark.sql.storeAssignmentPolicy": "legacy",
                "spark.yarn.maxAppAttempts": "1"
            },

            "args": ["workflow=" + WORKFLOW_NAME, "runmode=global", "BASEDIVNBR=1", "geo_rgn_cd=" + GEO_RGN_CD,
                     "op_cmpny_cd=" + OP_CMPNY_CD, "Schema=" + SCHEMA_NAME, "Table=" + TABLE_NAME,
                     "enableservices=DATAQUALITY,AUDIT", "runDQ=true", "env=" + ENV, "TgtTable=" + TGT_TABLE,
                     "CatlgTable=" + TGT_TABLE, "location=" + GCP_LOCATION, "src_rcv_dt=" + SRCRCVDT
                     ],

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml"
                          ]
        }
    }


repl_dmdunit_dly_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_dmdunit_dly_spark_load_job', job=create_spark_job("REPL_DMDUNIT_DLY"),
    gcp_conn_id=CONN_ID, dag=repl_dmdunit_dly_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=repl_dmdunit_dly_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

# **************** HDFS File archive Task started ******************
# sh /edge_data/code/svcmdsedat/availability/resources/scripts/GCP_Archive_SourceFile.sh &LANDING_HDFS_PATH# &ARCHIVE_HDFS_PATH# &LOAD_TYPE# &COUNTRY_CODE# &SCHEMA_NAME# &TABLE_PATH_WMT# &SRCRCVDT#

repl_dmdunit_dly_file_archive_in_hdfs = SSHOperator(
    task_id="file_archive_in_hdfs",
    ssh_hook=edgenode_sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="sh /edge_data/code/svcmdsedat/availability/resources/scripts/GCP_Archive_SourceFile.sh  " + LANDING_HDFS_PATH + " " + ARCHIVE_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_UPSERT + " " + SRCRCVDT,
    dag=repl_dmdunit_dly_load_dag,
    trigger_rule='all_success'
)

# command_2 = "gcloud auth activate-service-account --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; gsutil cat gs://dca-repl-airflow-dag/files/GCP_Archive_SourceFile.sh  | sh -s " + LANDING_HDFS_PATH + " " + ARCHIVE_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_PATH_UPSERT + " " + SRCRCVDT
#
# repl_dmdunit_dly_file_archive_in_hdfs = BashOperator(
#     task_id="file_archive_in_hdfs",
#     bash_command=command_2,
#     dag=repl_dmdunit_dly_load_dag,
#     trigger_rule='all_success'
# )

# **************** Delete Oldest file from NFS mount Task started ******************
repl_dmdunit_dly_delete_oldest_file = SSHOperator(
    task_id="delete_oldest_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  '" + OLDEST_FILE + "';",
    dag=repl_dmdunit_dly_load_dag,
    trigger_rule='all_success'
)

# **************** Delete Signal file from NFS mount Task started ******************
repl_dmdunit_dly_delete_signal_file = SSHOperator(
    task_id="delete_signal_file",
    ssh_hook=sshHook,
    conn_timeout=600, cmd_timeout=600,
    command="export AORTA_HOME='/u/appaorta/aorta/bin/framework/'; sh /u/appaorta/aorta/scripts/Aorta_Delete_Oldest_Bin_File_Name.sh  " + SRC_SIGNAL_FILE_FULL_NAME + ";",
    dag=repl_dmdunit_dly_load_dag,
    on_success_callback=success_email_function,
    trigger_rule='all_success'
)

repl_dmdunit_dly_common_event_poll >> repl_dmdunit_dly_oldest_file_check >> assign_variable >> repl_dmdunit_dly_file_move_to_hdfs >> create_bfdms_dpaas_cluster >> repl_dmdunit_dly_spark_load_job >> repl_dmdunit_dly_file_archive_in_hdfs >> repl_dmdunit_dly_delete_oldest_file >> repl_dmdunit_dly_delete_signal_file
repl_dmdunit_dly_spark_load_job >> delete_cluster
