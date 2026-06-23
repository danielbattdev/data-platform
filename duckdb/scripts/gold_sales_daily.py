import duckdb

con = duckdb.connect()

con.execute("""
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET minio_secret (
TYPE S3,
PROVIDER config,
KEY_ID 'minioadmin',
SECRET 'minioadmin123',
ENDPOINT 'localhost:9000',
USE_SSL false,
URL_STYLE 'path'
);
""")

con.execute("""

COPY (

SELECT

    DATE(o.purchase_timestamp)
    AS sale_date,

    COUNT(DISTINCT o.order_id)
    AS total_orders,

    SUM(p.payment_value)
    AS total_revenue,

    AVG(p.payment_value)
    AS average_ticket


FROM read_parquet(
's3://silver/olist/orders.parquet'
) o


JOIN read_parquet(
's3://silver/olist/payments.parquet'
) p

ON o.order_id = p.order_id


GROUP BY 1


)

TO
's3://gold/olist/sales_daily.parquet'

(FORMAT PARQUET);

""")

print("Gold sales daily criada")

con.close()

