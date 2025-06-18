from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.utils.email import send_email
from bfdms.dpaas import BFDMSDataprocSubmitJobOperator as DataprocSubmitJobOperator
from bfdms.dpaas import BFDMSDataprocDeleteClusterOperator as DataprocDeleteClusterOperator
from bfdms.dpaas import BFDMSDataprocCreateClusterOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

SERVICE_ACCOUNT = Variable.get("service_account")
CONN_ID = Variable.get("common_variables", deserialize_json=True)["connection_id"]
ENV = Variable.get("common_variables", deserialize_json=True)["env"]
EMAIL_ID = Variable.get("common_variables", deserialize_json=True)["email_id"]
SUCCESS_MSG = "dc_priority_item_dag table load was successful"

CLUSTER_NAME = "repl-dl-ephe-repl-dc-priority-item"

SCHEMA_NAME = Variable.get("dc_priority_item_dag_var", deserialize_json=True)["schema_name"]
TABLE_NAME = Variable.get("dc_priority_item_dag_var", deserialize_json=True)["table_name"]

# SCHEMA_NAME = "ww_repl_dl_secure"
# TABLE_NAME = "repl_dc_priority_item"
# SCHEMA_NAME = "stg_ww_repl_dl_secure"
# TABLE_NAME = "repl_dc_priority_item_test2"
REPL_JAR_PATH = "gs://dca-repl-airflow-dag/JARS/"
REPL_FILE_PATH = "gs://dca-repl-airflow-dag/files/"

DAG_NAME = "dc_priority_item_dag"
DAG_DESC = "dc_priority_item_dag table load job"
APP_NAME = "dc_priority_item_dag"

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
                "boot_disk_size_gb": 200
            }
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-16",
            "disk_config": {
                "boot_disk_size_gb": 200
            }
        },
        "secondary_worker_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-16",
            "disk_config": {
                "boot_disk_size_gb": 200
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


def decode_variable(ti):
    cmd_output = str(ti.xcom_pull(task_ids='read_db2_credential'))
    print("########### OUTPUT ############")
    print(cmd_output)

    SSL_TRUST_STORE_LOCATION_1 = cmd_output.split(";")[0]
    SSL_KEY_STORE_LOCATION_1 = cmd_output.split(";")[1]
    SSL_KEY_STORE_PASSWORD_1 = cmd_output.split(";")[2]
    USER_1 = cmd_output.split(";")[3]
    URL_1 = cmd_output.split(";")[4]
    DRIVER_1 = cmd_output.split(";")[5]

    ti.xcom_push(key="SSL_TRUST_STORE_LOCATION_1", value=SSL_TRUST_STORE_LOCATION_1)
    ti.xcom_push(key="SSL_KEY_STORE_LOCATION_1", value=SSL_KEY_STORE_LOCATION_1)
    ti.xcom_push(key="SSL_KEY_STORE_PASSWORD_1", value=SSL_KEY_STORE_PASSWORD_1)
    ti.xcom_push(key="USER_1", value=USER_1)
    ti.xcom_push(key="URL_1", value=URL_1)
    ti.xcom_push(key="DRIVER_1", value=DRIVER_1)


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

dc_priority_item_load_dag = DAG(
    DAG_NAME,
    description=DAG_DESC,
    default_args=default_args,
    schedule_interval='35 11 * * *',
    start_date=days_ago(1),
    max_active_runs=1,
    concurrency=5,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['ReplDb2Module']
)

create_bfdms_dpaas_cluster = BFDMSDataprocCreateClusterOperator(
    task_id='create_bfdms_dpaas_cluster',
    cluster_name=CLUSTER_NAME, dag=dc_priority_item_load_dag,
    dpaas_config=dpaas_config,
    gcp_conn_id=CONN_ID)

command_1 = "gcloud auth activate-service-account mdsereplprd-prod-sa1@wmt-bfdms-mdsereplprd.iam.gserviceaccount.com --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; gsutil cat gs://dca-repl-airflow-dag/files/db2_cert_file_pwd.txt"

read_db2_credential = BashOperator(
    task_id='read_db2_credential',
    bash_command=command_1,
    dag=dc_priority_item_load_dag,
    trigger_rule='all_success'
)

parse_output = PythonOperator(
    task_id='parse_output',
    python_callable=decode_variable,
    provide_context=True,
    dag=dc_priority_item_load_dag,
    trigger_rule='all_success'
)

GCP_LOCATION = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/landing/incremental/us/" + SCHEMA_NAME + "/" + TABLE_NAME + "/src_rcv_dt"

SRCRCVDT = str(date.today())

LANDING_HDFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/landing"
ARCHIVE_HDFS_PATH = "gs://d44d65161c7aa338e992de5105bdae5790ad263e56accf675683dbd507f1bd/user/svcmdsedat/raw"
LOAD_TYPE = "incremental"
COUNTRY_CODE = "us"

SSL_TRUST_STORE_LOCATION = "{{ ti.xcom_pull(task_ids='parse_output', key='SSL_TRUST_STORE_LOCATION_1') }}"
SSL_KEY_STORE_LOCATION = "{{ ti.xcom_pull(task_ids='parse_output', key='SSL_KEY_STORE_LOCATION_1') }}"
SSL_KEY_STORE_PASSWORD = "{{ ti.xcom_pull(task_ids='parse_output', key='SSL_KEY_STORE_PASSWORD_1') }}"
USER = "{{ ti.xcom_pull(task_ids='parse_output', key='USER_1') }}"
URL = "{{ ti.xcom_pull(task_ids='parse_output', key='URL_1') }}"
DRIVER = "{{ ti.xcom_pull(task_ids='parse_output', key='DRIVER_1') }}"
QUERY = "(SELECT PRIME_ITEM_NBR,DC_NBR,EXPIRE_IND,INBOUND_IND,EFFECTIVE_DATE,EXPIRATION_DATE,PRIORITY_ITEM_NBR,PRIORITY_DESC,CREATE_USERID,CREATE_TS,LAST_CHANGE_USERID, LAST_CHANGE_TS FROM DPMDS007.DC_PRIORITY_ITEM)"


def create_spark_job(WORKFLOW_NAME):
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "spark_job": {
            "jar_file_uris": [REPL_JAR_PATH + "Repl_DB2_Module/ReplDb2Module-1.110.jar",
                              REPL_JAR_PATH + "ScalaSparkArchetypeCore_2.12_3.3.0-3.0.8-bundled.jar",
                              REPL_JAR_PATH + "json4s-ext_2.12-3.5.3.jar",
                              REPL_JAR_PATH + "Repl_DB2_Module/db2jcc4.jar"
                              ],
            "main_class": "com.walmart.merc.replenishment.WorkFlowController",
            "properties": {
                "spark.driver.memory": "2g",
                "spark.executor.memory": "4g",
                "spark.executor.cores": "4",
                "spark.hadoop.hive.exec.dynamic.partition": "true",
                "spark.sql.hive.convertMetastoreOrc": "true",
                "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.executor.instances": "10",
                "spark.submit.deployMode": "cluster",
                "spark.dynamicAllocation.enabled": "false",
                "spark.sql.autoBroadcastJoinThreshold": "-1",
                "spark.sql.legacy.charVarcharAsString": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.shuffle.partitions": "400",
                "spark.yarn.executor.memoryOverhead": "3g",
                "spark.yarn.driver.memoryOverhead": "3g",
                "spark.driver.extraJavaOptions": "-Dudp.conn.file=connection.yaml -Dudp.job.file=archetype_spark.yaml"

            },

            "args": ["workflow=" + WORKFLOW_NAME, "runmode=global", "Schema=" + SCHEMA_NAME,
                     "TABLE_NAME=" + TABLE_NAME, "GCP_Location=" + GCP_LOCATION, "SRCRCVDT=" + SRCRCVDT,
                     "runtype=standalone", "env=" + ENV, "run_dq=true", "enableservices=Dataquality",
                     "securityMechanism=18", "sslTrustStoreLocation=" + SSL_TRUST_STORE_LOCATION,
                     "sslKeyStoreLocation=" + SSL_KEY_STORE_LOCATION, "sslKeyStoreType=PKCS12",
                     "sslKeyStorePassword=" + SSL_KEY_STORE_PASSWORD, "sslConnection=true", "user=" + USER,
                     "url=" + URL, "driver=" + DRIVER, "SourceType=DB2", "query=" + QUERY, "numPartitions=4",
                     "partitionColumn=DC_NBR"
                     ],

            "file_uris": [REPL_FILE_PATH + "log4j.properties",
                          REPL_FILE_PATH + "archetype_spark.yaml",
                          REPL_FILE_PATH + "connection.yaml",
                          REPL_JAR_PATH + "Repl_DB2_Module/db2_certs/cacert-walmart-sha256.jks",
                          REPL_JAR_PATH + "Repl_DB2_Module/db2_certs/GM2PDDO.wal-mart.com.p12"
                          ]
        }
    }


repl_dc_priority_item_spark_load_job = DataprocSubmitJobOperator(
    task_id='repl_dc_priority_item_spark_load_job', job=create_spark_job("repl_dc_priority_item"),
    gcp_conn_id=CONN_ID, dag=dc_priority_item_load_dag, trigger_rule='all_success')

delete_cluster = DataprocDeleteClusterOperator(task_id='delete_cluster', cluster_name=CLUSTER_NAME,
                                               dag=dc_priority_item_load_dag,
                                               trigger_rule='all_done', gcp_conn_id=CONN_ID
                                               )

command_2 = "gcloud auth activate-service-account --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; gsutil cat gs://dca-repl-airflow-dag/files/GCP_Archive_SourceFile.sh  | sh -s " + LANDING_HDFS_PATH + " " + ARCHIVE_HDFS_PATH + " " + LOAD_TYPE + " " + COUNTRY_CODE + " " + SCHEMA_NAME + " " + TABLE_NAME + " " + SRCRCVDT

repl_dc_priority_item_file_archive_in_hdfs = BashOperator(
    task_id="file_archive_in_hdfs",
    bash_command=command_2,
    dag=dc_priority_item_load_dag,
    trigger_rule='all_success'
)


TOUCH_DATE = str(date.today())
TOUCH_FILE = "gs://repl-prod-handshake-files/ReplDb2Module/" + TABLE_NAME + "/DONE@" + TOUCH_DATE
# gs://repl-prod-handshake-files/ReplDb2Module/repl_dc_priority_item/DONE@2025-01-16
command_3 = "gcloud auth activate-service-account --key-file=/usr/local/airflow/dags_mount/dags/mdsereplprd-prod-sa1-key.json; gcloud auth list; gsutil cat gs://dca-repl-airflow-dag/files/gcs_touch_file_creation.sh  | sh -s " + TOUCH_FILE

repl_dc_priority_item_touch_file_creation = BashOperator(
    task_id="repl_dc_priority_item_touch_file_creation",
    bash_command=command_3,
    dag=dc_priority_item_load_dag,
    trigger_rule='all_success',
    on_success_callback=success_email_function
)


create_bfdms_dpaas_cluster >> read_db2_credential >> parse_output >> repl_dc_priority_item_spark_load_job >> repl_dc_priority_item_file_archive_in_hdfs
repl_dc_priority_item_spark_load_job >> delete_cluster >> repl_dc_priority_item_touch_file_creation
