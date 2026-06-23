from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'daniel',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def task_ingestao_bronze():
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE S3, PROVIDER config,
            KEY_ID 'minioadmin',
            SECRET 'minioadmin123',
            ENDPOINT 'minio:9000',
            USE_SSL false,
            URL_STYLE 'path'
        )
    """)
    datasets = {
        '/opt/airflow/datasets/raw/olist/olist_orders_dataset.csv':
            's3://bronze/olist/orders/orders.parquet',
        '/opt/airflow/datasets/raw/olist/olist_customers_dataset.csv':
            's3://bronze/olist/customers/customers.parquet',
        '/opt/airflow/datasets/raw/olist/olist_products_dataset.csv':
            's3://bronze/olist/products/products.parquet',
        '/opt/airflow/datasets/raw/olist/olist_order_items_dataset.csv':
            's3://bronze/olist/order_items/order_items.parquet',
    }
    for csv_path, s3_path in datasets.items():
        con.execute(f"""
            COPY (SELECT * FROM read_csv_auto('{csv_path}', header=true))
            TO '{s3_path}' (FORMAT PARQUET, COMPRESSION 'snappy')
        """)
        print(f"✅ {csv_path} → {s3_path}")
    con.close()

def task_bronze_silver():
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE S3, PROVIDER config,
            KEY_ID 'minioadmin',
            SECRET 'minioadmin123',
            ENDPOINT 'minio:9000',
            USE_SSL false,
            URL_STYLE 'path'
        )
    """)
    con.execute("""
        COPY (
            SELECT
                order_id,
                customer_id,
                LOWER(TRIM(order_status))                       AS order_status,
                CAST(order_purchase_timestamp AS TIMESTAMP)     AS purchase_ts,
                CAST(order_approved_at AS TIMESTAMP)            AS approved_ts,
                CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_ts,
                CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_ts,
                DATEDIFF('day',
                    CAST(order_purchase_timestamp AS TIMESTAMP),
                    CAST(order_delivered_customer_date AS TIMESTAMP)
                ) AS days_to_deliver
            FROM read_parquet('s3://bronze/olist/orders/orders.parquet')
            WHERE order_id IS NOT NULL
              AND customer_id IS NOT NULL
              AND order_purchase_timestamp IS NOT NULL
        ) TO 's3://silver/olist/orders/orders_clean.parquet'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    print("✅ Silver orders criada.")
    con.execute("""
        COPY (
            SELECT
                customer_id,
                customer_unique_id,
                TRIM(customer_city) AS city,
                UPPER(TRIM(customer_state))  AS state
            FROM read_parquet('s3://bronze/olist/customers/customers.parquet')
            WHERE customer_id IS NOT NULL
        ) TO 's3://silver/olist/customers/customers_clean.parquet'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    print("✅ Silver customers criada.")
    con.close()

def task_silver_gold():
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE S3, PROVIDER config,
            KEY_ID 'minioadmin',
            SECRET 'minioadmin123',
            ENDPOINT 'minio:9000',
            USE_SSL false,
            URL_STYLE 'path'
        )
    """)
    con.execute("""
        COPY (
            SELECT
                DATE_TRUNC('month', purchase_ts)        AS mes_referencia,
                COUNT(DISTINCT order_id)                AS total_pedidos,
                COUNT(DISTINCT customer_id)             AS total_clientes_unicos,
                ROUND(AVG(days_to_deliver), 2)          AS media_dias_entrega,
                SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS pedidos_entregues,
                SUM(CASE WHEN order_status = 'canceled'  THEN 1 ELSE 0 END) AS pedidos_cancelados,
                ROUND(
                    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) * 100.0 /
                    COUNT(*), 2
                ) AS taxa_entrega_pct
            FROM read_parquet('s3://silver/olist/orders/orders_clean.parquet')
            GROUP BY 1
            ORDER BY 1
        ) TO 's3://gold/olist/vendas_mensais/vendas_mensais.parquet'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    print("✅ Gold: vendas_mensais criada.")
    con.execute("""
        COPY (
            SELECT
                c.state                         AS estado,
                COUNT(DISTINCT o.order_id)      AS total_pedidos,
                COUNT(DISTINCT o.customer_id)   AS total_clientes,
                ROUND(AVG(o.days_to_deliver), 2) AS media_dias_entrega
            FROM read_parquet('s3://silver/olist/orders/orders_clean.parquet') o
            JOIN read_parquet('s3://silver/olist/customers/customers_clean.parquet') c
              ON o.customer_id = c.customer_id
            WHERE o.order_status = 'delivered'
            GROUP BY 1
            ORDER BY 2 DESC
        ) TO 's3://gold/olist/performance_estados/performance_estados.parquet'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    print("✅ Gold: performance_estados criada.")
    con.close()

with DAG(
    dag_id='pipeline_olist',
    default_args=default_args,
    description='Pipeline completo: Bronze → Silver → Gold',
    start_date=datetime(2024, 1, 1),
    schedule='0 6 * * *',
    catchup=False,
    tags=['olist', 'pipeline'],
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    ingestao = PythonOperator(
        task_id='ingestao_bronze',
        python_callable=task_ingestao_bronze,
    )

    bronze_silver = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=task_bronze_silver,
    )

    silver_gold = PythonOperator(
        task_id='silver_to_gold',
        python_callable=task_silver_gold,
    )

    fim = EmptyOperator(task_id='fim')

    inicio >> ingestao >> bronze_silver >> silver_gold >> fim
