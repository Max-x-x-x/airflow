import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
FILE_TEMPLATE = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
URL_TEMPLATE = URL_PREFIX + FILE_TEMPLATE
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/'+FILE_TEMPLATE
TABLE_NAME_TEMPLATE = 'fhv_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'



# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2021,1,1),
    "end_date": datetime(2021,5,1)
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="homework_dag",
    schedule_interval="0 6 5 * *",
    default_args=default_args
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset",
        bash_command=f'wget {URL_TEMPLATE} -O {OUTPUT_FILE_TEMPLATE}'
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{FILE_TEMPLATE}",
            "local_file": OUTPUT_FILE_TEMPLATE,
        }
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": TABLE_NAME_TEMPLATE,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{FILE_TEMPLATE}"]
            }
        }
    )
    
    delete_dataset_task = BashOperator(
        task_id = 'delete_dataset',
        bash_command = f'rm {OUTPUT_FILE_TEMPLATE}'
    )
    

    download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task >> delete_dataset_task