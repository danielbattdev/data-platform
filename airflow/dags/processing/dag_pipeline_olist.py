from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

default_args = {
    'owner': 'daniel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def task_ingestao():
    sys.path.append('/opt/airflow/duckdb/scripts')
    from ingest import csv_to_parquet_bronze
    import os
    base = '/opt/airflow/datasets/raw/olist'
    datasets = {
        f'{base}/olist_orders_dataset.csv':      's3://bronze/olist/orders/orders.parquet',
        f'{base}/olist_customers_dataset.csv':   's3://bronze/olist/customers/customers.parquet',
        f'{base}/olist_products_dataset.csv':    's3://bronze/olist/products/products.parquet',
        f'{base}/olist_order_items_dataset.csv': 's3://bronze/olist/order_items/order_items.parquet',
    }
    for csv, s3 in datasets.items():
        csv_to_parquet_bronze(csv, s3)

def task_silver():
    sys.path.append('/opt/airflow/duckdb/scripts')
    import duckdb
    from transform_silver import configure_duckdb_s3, bronze_to_silver_orders, bronze_to_silver_customers
    con = duckdb.connect()
    configure_duckdb_s3(con)
    bronze_to_silver_orders(con)
    bronze_to_silver_customers(con)

def task_gold():
    sys.path.append('/opt/airflow/duckdb/scripts')
    import duckdb
    from transform_gold import configure_duckdb_s3, silver_to_gold_vendas_mensais, silver_to_gold_performance_estados
    con = duckdb.connect()
    configure_duckdb_s3(con)
    silver_to_gold_vendas_mensais(con)
    silver_to_gold_performance_estados(con)

with DAG(
    dag_id='pipeline_olist',
    default_args=default_args,
    description='Bronze → Silver → Gold',
    start_date=datetime(2024, 1, 1),
    schedule='0 6 * * *',
    catchup=False,
    tags=['olist', 'pipeline'],
) as dag:

    ingestao   = PythonOperator(task_id='ingestao_bronze',  python_callable=task_ingestao)
    bronze_slv = PythonOperator(task_id='bronze_to_silver', python_callable=task_silver)
    silver_gld = PythonOperator(task_id='silver_to_gold',   python_callable=task_gold)

    ingestao >> bronze_slv >> silver_gld
