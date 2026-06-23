import duckdb
import os

def configure_duckdb_s3(con):
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin123';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

def bronze_to_silver_orders(con):
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

def bronze_to_silver_customers(con):
    con.execute("""
        COPY (
            SELECT
                customer_id,
                customer_unique_id,
                TRIM(customer_city) AS city,
                UPPER(TRIM(customer_state))  AS state
            FROM read_parquet('s3://bronze/olist/customers/customers.parquet')
            WHERE customer_id IS NOT NULL
              AND customer_unique_id IS NOT NULL
        ) TO 's3://silver/olist/customers/customers_clean.parquet'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    print("✅ Silver customers criada.")

if __name__ == '__main__':
    con = duckdb.connect()
    configure_duckdb_s3(con)
    bronze_to_silver_orders(con)
    bronze_to_silver_customers(con)
    print("\n🎉 Silver completa!")
