import os
from dotenv import load_dotenv

load_dotenv()

import yfinance as yf
import psycopg2
from datetime import datetime

TICKERS = ['AAPL', 'MSFT', 'HSIC']

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'dbname': os.environ.get('DB_NAME', 'airflow'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
}

def collect_and_save():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for ticker in TICKERS:
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            cur.execute("""
                INSERT INTO stock_prices (ticker, price, open, high, low, volume, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                ticker,
                round(float(info['lastPrice']), 4),
                round(float(info['open']), 4),
                round(float(info['dayHigh']), 4),
                round(float(info['dayLow']), 4),
                int(info['lastVolume']),
                datetime.now()
            ))
            print(f"✅ {ticker} → ${info['lastPrice']:.2f}")
        except Exception as e:
            print(f"❌ {ticker}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("🎉 Coleta concluída!")

if __name__ == '__main__':
    collect_and_save()
