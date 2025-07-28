import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from db import create_table, upsert_stock_prices, get_latest_date
from data_models import StockPrice
from typing import List, Optional
from datetime import datetime, timedelta
import logging
import sys
import psycopg2
import os
from dotenv import load_dotenv
import pickle
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

load_dotenv()
DB_URL = os.getenv('DATABASE_URL')

def table_exists(table_name: str) -> bool:
    try:
        conn = psycopg2.connect(DB_URL)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            exists = cur.fetchone()[0]
        conn.close()
        return exists
    except Exception as e:
        logging.error(f"Error checking table existence: {e}")
        return False

def get_db_symbols() -> List[str]:
    try:
        conn = psycopg2.connect(DB_URL)
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT symbol FROM stock_price;")
            rows = cur.fetchall()
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        logging.error(f"Error fetching symbols from db: {e}")
        return []

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=2, max=30), retry=retry_if_exception_type(Exception))
def fetch_stock_history(symbol: str, start: str, end: str) -> pd.DataFrame:
    try:
        df = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=False)
        if df.empty:
            raise ValueError(f"No data returned for {symbol}")
        df.columns = df.columns.droplevel(1)
        df = df.reset_index()
        df['Symbol'] = symbol
        df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Adj Close': 'adj_close',
            'Volume': 'volume',
            'Symbol': 'symbol',
        }, inplace=True)
        return df[['symbol', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        raise

def validate_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    valid_rows = []
    for _, row in df.iterrows():
        try:
            validated = StockPrice(
                symbol=row['symbol'],
                date=row['date'].date() if hasattr(row['date'], 'date') else row['date'],
                open=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close'],
                adj_close=row['adj_close'],
                volume=row['volume']
            )
            valid_rows.append(validated.dict())
        except Exception as e:
            logging.warning(f"Invalid row for {row['symbol']} on {row['date']}: {e}")
    return pd.DataFrame(valid_rows)

def get_all_us_symbols() -> List[str]:
    tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    symbols = [symbol.replace('.', '') for symbol in tickers['Symbol'].to_list() if symbol != 'BRK.B']
    with open('temp/symbols.pkl', 'wb') as f:
        pickle.dump(symbols, f)
    return symbols

def run_pipeline(symbols: Optional[List[str]] = None, incremental: bool = True):
    if not table_exists('stock_price'):
        create_table()
    all_sp_symbols = get_all_us_symbols()
    db_symbols = get_db_symbols()
    missing_symbols = list(set(all_sp_symbols) - set(db_symbols))
    if missing_symbols:
        logging.info(f"Found {len(missing_symbols)} new S&P 500 symbols not in db. Adding their price history.")
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=5*365)
        for symbol in missing_symbols:
            try:
                df = fetch_stock_history(symbol, str(start_date), str(end_date + timedelta(days=1)))
                logging.info(f"{symbol} downloaded {df.shape[0]} rows, {df.shape[1]} columns (new symbol)")
                df_valid = validate_stock_data(df)
                logging.info(f"{symbol} validated {df_valid.shape[0]} rows, {df_valid.shape[1]} columns (new symbol)")
                upsert_stock_prices(df_valid)
                logging.info(f"Processed {symbol}: {len(df_valid)} rows (new symbol)")
            except Exception as e:
                logging.error(f"Failed to process new symbol {symbol}: {e}")
    # Continue with normal pipeline for all or provided symbols
    if symbols is None:
        symbols = all_sp_symbols
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=5*365)
    for symbol in symbols:
        try:
            if incremental:
                latest = get_latest_date(symbol)
                fetch_start = (latest + timedelta(days=1)) if latest else start_date
            else:
                fetch_start = start_date
            if fetch_start > end_date:
                logging.info(f"No new data for {symbol}")
                continue
            df = fetch_stock_history(symbol, str(fetch_start), str(end_date + timedelta(days=1)))
            logging.info(f"{symbol} downloaded {df.shape[0]} rows, {df.shape[1]} columns")
            df_valid = validate_stock_data(df)
            logging.info(f"{symbol} validated {df_valid.shape[0]} rows, {df_valid.shape[1]} columns")
            upsert_stock_prices(df_valid)
            logging.info(f"Processed {symbol}: {len(df_valid)} rows")
        except Exception as e:
            logging.error(f"Failed to process {symbol}: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="US Stock Price Data Pipeline")
    parser.add_argument('--symbol', type=str, help='Run pipeline for a single stock symbol')
    parser.add_argument('--full', action='store_true', help='Run full (not incremental) update')
    args = parser.parse_args()
    if args.symbol:
        run_pipeline([args.symbol], incremental=not args.full)
    else:
        run_pipeline(incremental=not args.full) 