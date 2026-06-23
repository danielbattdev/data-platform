import duckdb

resultado = duckdb.sql("""
SELECT COUNT(*)
FROM read_csv_auto(
'/home/daniel/data-platform/datasets/raw/olist/olist_orders_dataset.csv'
)
""").fetchone()[0]

print(f"Total de pedidos: {resultado:,}")

