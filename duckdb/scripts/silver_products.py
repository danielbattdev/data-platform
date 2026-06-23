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

    product_id,

    product_category_name,

    CAST(product_name_lenght AS INTEGER)
    AS name_length,

    CAST(product_description_lenght AS INTEGER)
    AS description_length,

    CAST(product_photos_qty AS INTEGER)
    AS photos_qty,

    CAST(product_weight_g AS DOUBLE)
    AS weight_g,

    CAST(product_length_cm AS DOUBLE)
    AS length_cm,

    CAST(product_height_cm AS DOUBLE)
    AS height_cm,

    CAST(product_width_cm AS DOUBLE)
    AS width_cm,

    CURRENT_TIMESTAMP
    AS silver_processed_at


FROM read_parquet(
's3://bronze/olist/olist_products_dataset.parquet'
)


WHERE product_id IS NOT NULL


)

TO
's3://silver/olist/products.parquet'

(FORMAT PARQUET);

""")

print("Silver products criada")

con.close()

