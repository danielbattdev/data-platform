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

    oi.product_id,

    p.product_category_name,

    COUNT(*) 
    AS total_items_sold,

    SUM(oi.price)
    AS total_revenue,

    AVG(oi.price)
    AS average_price


FROM read_parquet(
's3://bronze/olist/olist_order_items_dataset.parquet'
) oi


JOIN read_parquet(
's3://silver/olist/products.parquet'
) p

ON oi.product_id = p.product_id


GROUP BY
1,2


)

TO
's3://gold/olist/products_performance.parquet'

(FORMAT PARQUET);

""")

print("Gold products performance criada")

con.close()
