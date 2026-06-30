import duckdb
import os
from dotenv import load_dotenv

load_dotenv(os.path.expanduser('~/data-platform/.env'))

def configure_duckdb_s3(con):
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{os.getenv("MINIO_ENDPOINT").replace("http://","")}';
        SET s3_access_key_id='{os.getenv("MINIO_ACCESS_KEY")}';
        SET s3_secret_access_key='{os.getenv("MINIO_SECRET_KEY")}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

def csv_to_parquet_bronze(csv_path, s3_output):
    con = duckdb.connect()
    configure_duckdb_s3(con)
    con.execute(f"""
        COPY (
            SELECT * FROM read_csv_auto('{csv_path}', header=true)
        ) TO '{s3_output}'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    print(f"✅ {os.path.basename(csv_path)} → {s3_output}")

if __name__ == '__main__':
    base = os.path.expanduser('~/data-platform/datasets/raw/olist')
    datasets = {
        f'{base}/olist_orders_dataset.csv':      's3://bronze/olist/orders/orders.parquet',
        f'{base}/olist_customers_dataset.csv':   's3://bronze/olist/customers/customers.parquet',
        f'{base}/olist_products_dataset.csv':    's3://bronze/olist/products/products.parquet',
        f'{base}/olist_order_items_dataset.csv': 's3://bronze/olist/order_items/order_items.parquet',
    }
    for csv, s3 in datasets.items():
        csv_to_parquet_bronze(csv, s3)
