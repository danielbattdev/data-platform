from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import logging

MINIO_ENDPOINT  = "minio:9000"
MINIO_KEY       = "minioadmin"
MINIO_SECRET    = "minioadmin123"

default_args = {
    'owner': 'daniel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def configurar_duckdb():
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

def task_bronze_to_silver():
    import duckdb
    con = configurar_duckdb()

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
    logging.info("✅ Silver orders criada.")

    con.execute("""
        COPY (
            SELECT
                customer_id,
                customer_unique_id,
                customer_city    AS city,
                customer_state   AS state
            FROM read_parquet('s3://bronze/olist/customers/customers.parquet')
            WHERE customer_id IS NOT NULL
        ) TO 's3://silver/olist/customers/customers_clean.parquet'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    logging.info("Silver customers criada.")

def task_silver_to_gold():
    import duckdb
    con = configurar_duckdb()

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
    logging.info("✅ Gold vendas_mensais criada.")

    con.execute("""
        COPY (
            SELECT
                c.state                             AS estado,
                COUNT(DISTINCT o.order_id)          AS total_pedidos,
                COUNT(DISTINCT o.customer_id)       AS total_clientes,
                ROUND(AVG(o.days_to_deliver), 2)    AS media_dias_entrega
            FROM read_parquet('s3://silver/olist/orders/orders_clean.parquet') o
            JOIN read_parquet('s3://silver/olist/customers/customers_clean.parquet') c
              ON o.customer_id = c.customer_id
            WHERE o.order_status = 'delivered'
            GROUP BY 1
            ORDER BY 2 DESC
        ) TO 's3://gold/olist/performance_estados/performance_estados.parquet'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    logging.info("✅ Gold performance_estados criada.")
    con.close()

def task_validar_gold():
    import duckdb
    con = configurar_duckdb()

    resultado = con.execute("""
        SELECT COUNT(*) AS total_meses
        FROM read_parquet('s3://gold/olist/vendas_mensais/vendas_mensais.parquet')
    """).fetchone()

    if resultado[0] == 0:
        raise ValueError("Gold vendas_mensais está vazia!")

    logging.info(f"✅ Validação OK — {resultado[0]} meses na Gold.")
    con.close()

with DAG(
    dag_id='pipeline_olist',
    default_args=default_args,
    description='Pipeline Olist: Bronze → Silver → Gold',
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule='0 6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['olist', 'pipeline', 'medallion'],
) as dag:

    t1 = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=task_bronze_to_silver,
    )

    t2 = PythonOperator(
        task_id='silver_to_gold',
        python_callable=task_silver_to_gold,
    )

    t3 = PythonOperator(
        task_id='validar_gold',
        python_callable=task_validar_gold,
    )

    t1 >> t2 >> t3
