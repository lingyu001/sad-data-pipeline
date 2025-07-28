import os
import psycopg2
from psycopg2 import pool, sql, extras
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Use DATABASE_URL for NeonDB connection string
DB_URL = os.getenv('DATABASE_URL')

# Create a connection pool
connection_pool = pool.SimpleConnectionPool(
    1,  # minconn
    10, # maxconn
    DB_URL
)

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS stock_price (
    symbol VARCHAR(16) NOT NULL,
    date DATE NOT NULL,
    open FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    close FLOAT NOT NULL,
    adj_close FLOAT NOT NULL,
    volume BIGINT,
    PRIMARY KEY (symbol, date)
);
'''

def create_table():
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()
    finally:
        connection_pool.putconn(conn)

def upsert_stock_prices(df: pd.DataFrame):
    if df.empty:
        return
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            insert_sql = '''
                INSERT INTO stock_price (symbol, date, open, high, low, close, adj_close, volume)
                VALUES %s
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open=EXCLUDED.open,
                    high=EXCLUDED.high,
                    low=EXCLUDED.low,
                    close=EXCLUDED.close,
                    adj_close=EXCLUDED.adj_close,
                    volume=EXCLUDED.volume;
            '''
            values = [
                (
                    row['symbol'],
                    row['date'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['adj_close'],
                    row['volume']
                ) for _, row in df.iterrows()
            ]
            extras.execute_values(cur, insert_sql, values, page_size=100)
            conn.commit()
    finally:
        connection_pool.putconn(conn)

def get_latest_date(symbol):
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT MAX(date) FROM stock_price WHERE symbol = %s', (symbol,))
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    finally:
        connection_pool.putconn(conn) 