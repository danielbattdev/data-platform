import duckdb

def configure_duckdb_s3(con):
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin123';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

def silver_to_gold_vendas_mensais(con):
    con.execute("""
        COPY (
            SELECT
                DATE_TRUNC('month', purchase_ts)        AS mes_referencia,
                COUNT(DISTINCT order_id)                AS total_pedidos,
                COUNT(DISTINCT customer_id)             AS total_clientes_unicos,
                ROUND(AVG(days_to_deliver), 2)          AS media_dias_entrega,
                SUM(CASE WHEN order_status = 'delivered'
                         THEN 1 ELSE 0 END)             AS pedidos_entregues,
                SUM(CASE WHEN order_status = 'canceled'
                         THEN 1 ELSE 0 END)             AS pedidos_cancelados,
                ROUND(
                    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) * 100.0 /
                    COUNT(*), 2
                )                                       AS taxa_entrega_pct
            FROM read_parquet('s3://silver/olist/orders/orders_clean.parquet')
            GROUP BY 1
            ORDER BY 1
        ) TO 's3://gold/olist/vendas_mensais/vendas_mensais.parquet'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    print("✅ Gold: vendas_mensais criada.")

def silver_to_gold_performance_estados(con):
    con.execute("""
        COPY (
            SELECT
                c.state                             AS estado,
                COUNT(DISTINCT o.order_id)          AS total_pedidos,
                COUNT(DISTINCT o.customer_id)       AS total_clientes,
                ROUND(AVG(o.days_to_deliver), 2)    AS media_dias_entrega
            FROM read_parquet('s3://silver/olist/orders/orders_clean.parquet') o
            JOIN read_parquet('s3://silver/olist/customers/customers_clean.parquet') c
              ON o.customer_id = c.customer_id
            WHERE o.order_status = 'delivered'
            GROUP BY 1
            ORDER BY 2 DESC
        ) TO 's3://gold/olist/performance_estados/performance_estados.parquet'
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    print("✅ Gold: performance_estados criada.")

if __name__ == '__main__':
    con = duckdb.connect()
    configure_duckdb_s3(con)
    silver_to_gold_vendas_mensais(con)
    silver_to_gold_performance_estados(con)
    print("\n🎉 Gold completa!")
