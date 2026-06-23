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

    order_id,

    order_status,

    purchase_timestamp,

    delivered_timestamp,

    estimated_delivery_timestamp,


    DATE_DIFF(
        'day',
        purchase_timestamp,
        delivered_timestamp
    ) AS delivery_days,


    DATE_DIFF(
        'day',
        estimated_delivery_timestamp,
        delivered_timestamp
    ) AS delay_days


FROM read_parquet(
's3://silver/olist/orders.parquet'
)


WHERE delivered_timestamp IS NOT NULL


)

TO
's3://gold/olist/delivery_metrics.parquet'

(FORMAT PARQUET);

""")

print("Gold delivery metrics criada")

con.close()

