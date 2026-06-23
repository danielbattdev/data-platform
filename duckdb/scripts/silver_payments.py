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

    CAST(payment_sequential AS INTEGER)
    AS payment_sequential,

    LOWER(TRIM(payment_type))
    AS payment_type,

    CAST(payment_installments AS INTEGER)
    AS installments,

    CAST(payment_value AS DOUBLE)
    AS payment_value,

    CURRENT_TIMESTAMP
    AS silver_processed_at


FROM read_parquet(
's3://bronze/olist/olist_order_payments_dataset.parquet'
)


WHERE order_id IS NOT NULL


)

TO
's3://silver/olist/payments.parquet'

(FORMAT PARQUET);

""")

print("Silver payments criada")

con.close()
