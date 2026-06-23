from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3


def upload_minio():

    client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )

    client.upload_file(
        "/opt/airflow/datasets/raw/clientes.csv",
        "lakehouse",
        "bronze/clientes.csv"
    )


with DAG(
    dag_id="ingestao_bronze",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    enviar_para_minio = PythonOperator(
        task_id="upload_para_bronze",
        python_callable=upload_minio
    )
