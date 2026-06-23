import duckdb
from pathlib import Path

RAW_PATH = Path("/home/daniel/data-platform/datasets/raw/olist")
BRONZE_PATH = Path("/home/daniel/data-platform/storage/bronze/olist")

BRONZE_PATH.mkdir(parents=True, exist_ok=True)

csv_files = list(RAW_PATH.glob("*.csv"))

for csv_file in csv_files:
    parquet_file = BRONZE_PATH / f"{csv_file.stem}.parquet"

    print(f"Convertendo {csv_file.name}...")

    duckdb.sql(f"""
        COPY (
            SELECT *
            FROM read_csv_auto('{csv_file}')
        )
        TO '{parquet_file}'
        (FORMAT PARQUET);
    """)

    print(f"✓ Gerado: {parquet_file.name}")

print("\nConversão concluída!")

