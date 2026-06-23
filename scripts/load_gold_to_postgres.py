import duckdb
import pandas as pd
from sqlalchemy import create_engine

# Conecta ao DuckDB
con = duckdb.connect()

# Configura acesso ao MinIO
con.execute("""
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

# Conexão PostgreSQL
engine = create_engine(
    "postgresql://airflow:airflow@localhost:5432/airflow"
)

print("Carregando vendas_mensais...")

df_vendas = con.execute("""
SELECT *
FROM read_parquet(
's3://gold/olist/vendas_mensais/vendas_mensais.parquet'
)
""").df()

df_vendas.to_sql(
    "gold_vendas_mensais",
    engine,
    if_exists="replace",
    index=False
)

print("Carregando performance_estados...")

df_estados = con.execute("""
SELECT *
FROM read_parquet(
's3://gold/olist/performance_estados/performance_estados.parquet'
)
""").df()

df_estados.to_sql(
    "gold_performance_estados",
    engine,
    if_exists="replace",
    index=False
)

print("Carga concluída com sucesso!")
