import duckdb

query = """
SELECT
    COUNT(*) AS total_pedidos
FROM read_parquet(
'/home/daniel/data-platform/storage/bronze/olist/olist_orders_dataset.parquet'
)
"""

resultado = duckdb.sql(query).fetchall()

print(resultado)

