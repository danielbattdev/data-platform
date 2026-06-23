import duckdb

con = duckdb.connect()

con.execute("""
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin123';
SET s3_use_ssl=false;
SET s3_url_style='path';
""")

con.execute("""

COPY (

SELECT

    customer_id,

    customer_unique_id,

    LPAD(
        CAST(customer_zip_code_prefix AS VARCHAR),
        5,
        '0'
    ) AS zip_code,

    LOWER(TRIM(customer_city))
    AS city,

    UPPER(TRIM(customer_state))
    AS state,

    CURRENT_TIMESTAMP
    AS silver_processed_at


FROM read_parquet(
's3://bronze/olist/olist_customers_dataset.parquet'
)


WHERE customer_id IS NOT NULL


)

TO
's3://silver/olist/customers.parquet'

(FORMAT PARQUET);

""")

print("Silver customers criada")

con.close()
