import duckdb
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.expanduser('~/data-platform/.env'))

def configure_duckdb_s3(con):
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{os.getenv("MINIO_ENDPOINT", "http://localhost:9000").replace("http://","")}';
        SET s3_access_key_id='{os.getenv("MINIO_ACCESS_KEY", "minioadmin")}';
        SET s3_secret_access_key='{os.getenv("MINIO_SECRET_KEY", "minioadmin123")}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

def get_postgres_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow")
    )

def load_vendas_mensais():
    con = duckdb.connect()
    configure_duckdb_s3(con)
    df = con.execute("""
        SELECT * FROM read_parquet('s3://gold/olist/vendas_mensais/vendas_mensais.parquet')
    """).fetchdf()

    pg = get_postgres_conn()
    cur = pg.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold_vendas_mensais (
            mes_referencia DATE,
            total_pedidos INT,
            total_clientes_unicos INT,
            media_dias_entrega NUMERIC,
            pedidos_entregues INT,
            pedidos_cancelados INT,
            taxa_entrega_pct NUMERIC
        )
    """)
    cur.execute("TRUNCATE TABLE gold_vendas_mensais")

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO gold_vendas_mensais
            (mes_referencia, total_pedidos, total_clientes_unicos, media_dias_entrega,
             pedidos_entregues, pedidos_cancelados, taxa_entrega_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    pg.commit()
    cur.close()
    pg.close()
    print(f"✅ gold_vendas_mensais carregada: {len(df)} registros")

def load_performance_estados():
    con = duckdb.connect()
    configure_duckdb_s3(con)
    df = con.execute("""
        SELECT * FROM read_parquet('s3://gold/olist/performance_estados/performance_estados.parquet')
    """).fetchdf()

    pg = get_postgres_conn()
    cur = pg.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold_performance_estados (
            estado VARCHAR(2),
            total_pedidos INT,
            total_clientes INT,
            media_dias_entrega NUMERIC
        )
    """)
    cur.execute("TRUNCATE TABLE gold_performance_estados")

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO gold_performance_estados
            (estado, total_pedidos, total_clientes, media_dias_entrega)
            VALUES (%s, %s, %s, %s)
        """, tuple(row))

    pg.commit()
    cur.close()
    pg.close()
    print(f"✅ gold_performance_estados carregada: {len(df)} registros")

if __name__ == '__main__':
    load_vendas_mensais()
    load_performance_estados()
    print("\n🎉 Carga Gold → PostgreSQL concluída!")
