from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import logging

PG_HOST     = "postgres"
PG_PORT     = 5432
PG_DBNAME   = "airflow"
PG_USER     = "airflow"
PG_PASSWORD = "airflow"

TICKERS = ["AAPL", "MSFT", "HSIC"]

def garantir_tabela():
    import psycopg2
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id           SERIAL PRIMARY KEY,
            ticker       VARCHAR(10)  NOT NULL,
            price        NUMERIC(12,4),
            open         NUMERIC(12,4),
            high         NUMERIC(12,4),
            low          NUMERIC(12,4),
            volume       BIGINT,
            collected_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_stock_ticker_time
        ON stock_prices (ticker, collected_at DESC);
    """)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Tabela stock_prices verificada/criada.")

def coletar_e_salvar():
    import yfinance as yf
    import psycopg2
    from datetime import datetime, timezone

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD
    )
    cur  = conn.cursor()
    now  = datetime.now(timezone.utc)
    erros = []

    for symbol in TICKERS:
        try:
            info  = yf.Ticker(symbol).fast_info
            price = getattr(info, "last_price",  None)
            open_ = getattr(info, "open",        None)
            high  = getattr(info, "day_high",    None)
            low   = getattr(info, "day_low",     None)
            vol   = getattr(info, "last_volume", None)

            if price is None:
                raise ValueError(f"Preco nulo para {symbol}")

            cur.execute("""
                INSERT INTO stock_prices
                    (ticker, price, open, high, low, volume, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (symbol, price, open_, high, low, vol, now))

            logging.info(f"{symbol} -> ${price:.2f}")

        except Exception as e:
            erros.append(f"{symbol}: {e}")
            logging.error(f"Erro em {symbol}: {e}")

    conn.commit()
    cur.close()
    conn.close()

    if erros:
        raise RuntimeError(f"Falhas: {'; '.join(erros)}")

    logging.info(f"Coleta finalizada — {len(TICKERS)} tickers.")

with DAG(
    dag_id="collect_stocks_15min",
    description="Coleta AAPL, MSFT, HSIC a cada 15 minutos",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "ingestao"],
) as dag:

    t1 = PythonOperator(
        task_id="garantir_tabela",
        python_callable=garantir_tabela,
    )

    t2 = PythonOperator(
        task_id="coletar_e_salvar",
        python_callable=coletar_e_salvar,
    )

    t1 >> t2
