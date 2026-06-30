import duckdb
import os
from dotenv import load_dotenv

load_dotenv(os.path.expanduser('~/data-platform/.env'))

def configure(con):
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{os.getenv("MINIO_ENDPOINT").replace("http://","")}';
        SET s3_access_key_id='{os.getenv("MINIO_ACCESS_KEY")}';
        SET s3_secret_access_key='{os.getenv("MINIO_SECRET_KEY")}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

def test_bronze_orders_nao_vazio():
    con = duckdb.connect()
    configure(con)
    result = con.execute("SELECT COUNT(*) as total FROM read_parquet('s3://bronze/olist/orders/orders.parquet')").fetchone()
    assert result[0] > 0, "Bronze vazio!"

def test_silver_sem_nulos():
    con = duckdb.connect()
    configure(con)
    result = con.execute("""
        SELECT SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as nulls
        FROM read_parquet('s3://silver/olist/orders/orders_clean.parquet')
    """).fetchone()
    assert result[0] == 0, "Silver tem nulos em order_id!"

def test_gold_vendas_mensais_completa():
    con = duckdb.connect()
    configure(con)
    result = con.execute("""
        SELECT COUNT(*) as meses, SUM(total_pedidos) as total
        FROM read_parquet('s3://gold/olist/vendas_mensais/vendas_mensais.parquet')
    """).fetchone()
    assert result[0] >= 24, "Gold com menos de 24 meses!"
    assert result[1] > 90000, "Total de pedidos inconsistente!"

def test_gold_performance_estados_completa():
    con = duckdb.connect()
    configure(con)
    result = con.execute("""
        SELECT COUNT(*) as estados
        FROM read_parquet('s3://gold/olist/performance_estados/performance_estados.parquet')
    """).fetchone()
    assert result[0] >= 20, "Menos de 20 estados no Gold!"

if __name__ == '__main__':
    test_bronze_orders_nao_vazio()
    print("✅ Bronze OK")
    test_silver_sem_nulos()
    print("✅ Silver OK")
    test_gold_vendas_mensais_completa()
    print("✅ Gold vendas_mensais OK")
    test_gold_performance_estados_completa()
    print("✅ Gold performance_estados OK")
    print("\n🎉 Todos os testes passaram!")
