import yfinance as yf
from pymongo import MongoClient, UpdateOne
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

# List of tickers to process
tickers = ohlcv_collection.distinct('ticker')

# Function to fetch and update daily OHLCV data
def fetch_daily_ohlcv_data():
    batch_size = 100  # Adjust based on resources
    for batch_start in range(0, len(tickers), batch_size):
        batch_tickers = tickers[batch_start:batch_start+batch_size]
        try:
            # Fetch the last 5 days of data for the batch
            data = yf.download(batch_tickers, period="5d", group_by='ticker', threads=True)
            if data.empty:
                continue
            # Prepare data for upsert
            bulk_operations = []
            if isinstance(data.columns, pd.MultiIndex):
                # Data is multi-indexed
                for ticker in batch_tickers:
                    if ticker not in data.columns.levels[1]:
                        logging.warning(f"No data for {ticker}")
                        continue
                    ticker_data = data.loc[:, pd.IndexSlice[:, ticker]]
                    ticker_data.columns = ticker_data.columns.droplevel(1)
                    ticker_data = ticker_data.dropna(how='all')
                    if ticker_data.empty:
                        logging.warning(f"No data for {ticker}")
                        continue
                    # Get the latest data point
                    latest_data = ticker_data.iloc[-1]
                    date = latest_data.name.to_pydatetime()
                    # Create the data document
                    data_doc = {
                        "ticker": ticker,
                        "date": date,
                        "open": latest_data['Open'],
                        "high": latest_data['High'],
                        "low": latest_data['Low'],
                        "close": latest_data['Close'],
                        "volume": latest_data['Volume'],
                    }
                    bulk_operations.append(UpdateOne(
                        {"ticker": ticker, "date": date},
                        {"$set": data_doc},
                        upsert=True
                    ))
            else:
                # Data is single-indexed (only one ticker)
                for ticker in batch_tickers:
                    if ticker != data.columns.levels[0][0]:
                        continue
                    ticker_data = data
                    if ticker_data.empty:
                        logging.warning(f"No data for {ticker}")
                        continue
                    # Get the latest data point
                    latest_data = ticker_data.iloc[-1]
                    date = latest_data.name.to_pydatetime()
                    # Create the data document
                    data_doc = {
                        "ticker": ticker,
                        "date": date,
                        "open": latest_data['Open'],
                        "high": latest_data['High'],
                        "low": latest_data['Low'],
                        "close": latest_data['Close'],
                        "volume": latest_data['Volume'],
                    }
                    bulk_operations.append(UpdateOne(
                        {"ticker": ticker, "date": date},
                        {"$set": data_doc},
                        upsert=True
                    ))
            # Perform bulk upsert
            if bulk_operations:
                ohlcv_collection.bulk_write(bulk_operations, ordered=False)
                logging.info(f"Upserted data for batch starting at index {batch_start}")
        except Exception as e:
            logging.error(f"Error fetching data for batch starting at index {batch_start}: {e}")
    logging.info("Daily OHLCV data updated successfully.")

# Function to calculate RS values (RS1, RS2, RS3, RS4) and daily percentage change
def calculate_rs_values():
    periods = {
        "RS1": 63,
        "RS2": 126,
        "RS3": 189,
        "RS4": 252
    }
    
    def process_ticker(ticker):
        try:
            # Fetch the last 252 + 1 days of data for the ticker
            cursor = ohlcv_collection.find(
                {"ticker": ticker},
                {"date": 1, "close": 1, "_id": 0}
            ).sort("date", -1).limit(252 + 1)
            history = list(cursor)
            if not history:
                logging.warning(f"No data found for {ticker}")
                return
            df = pd.DataFrame(history)
            df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last').reset_index(drop=True)
            # Compute daily_pct_change
            df['daily_pct_change'] = df['close'].pct_change() * 100
            # Compute RS values using shifted close prices
            for rs_key, period in periods.items():
                df[f'close_shift_{period}'] = df['close'].shift(period)
                df[rs_key] = (df['close'] - df[f'close_shift_{period}']) / df[f'close_shift_{period}'] * 100
            # Get the latest row
            current_row = df.iloc[-1]
            update_doc = {
                "daily_pct_change": current_row['daily_pct_change']
            }
            for rs_key in periods.keys():
                if pd.notnull(current_row[rs_key]):
                    update_doc[rs_key] = current_row[rs_key]
            # Update the latest date's data
            ohlcv_collection.update_one(
                {"ticker": ticker, "date": current_row['date']},
                {"$set": update_doc},
                upsert=True
            )
            logging.info(f"RS values updated for {ticker} on {current_row['date']}")
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
    
    # Use ThreadPoolExecutor to process tickers in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        list(tqdm(executor.map(process_ticker, tickers), total=len(tickers), desc="Calculating RS values"))
    logging.info("RS values and daily percentage change calculated.")

# Function to calculate and rank RS score using RS4, RS3, RS2, RS1 as fallbacks
def calculate_rs_ranking():
    # Fetch the latest date that has RS values
    latest_date_doc = ohlcv_collection.find_one(
        {"$or": [
            {"RS4": {"$exists": True, "$ne": None}},
            {"RS3": {"$exists": True, "$ne": None}},
            {"RS2": {"$exists": True, "$ne": None}},
            {"RS1": {"$exists": True, "$ne": None}},
        ]},
        sort=[("date", -1)],
        projection={"date": 1}
    )
    if not latest_date_doc:
        logging.error("No data with RS values found")
        return
    latest_date = latest_date_doc["date"]
    logging.info(f"Latest trading date for RS values: {latest_date}")
    # Fetch all stocks with RS values on the latest date
    cursor = ohlcv_collection.find(
        {"date": latest_date, "$or": [
            {"RS4": {"$exists": True, "$ne": None}},
            {"RS3": {"$exists": True, "$ne": None}},
            {"RS2": {"$exists": True, "$ne": None}},
            {"RS1": {"$exists": True, "$ne": None}},
        ]},
        {"ticker": 1, "RS4": 1, "RS3": 1, "RS2": 1, "RS1": 1}
    )
    df = pd.DataFrame(list(cursor))
    if df.empty:
        logging.error("No stocks found with RS values")
        return
    # Determine the RS value to use
    def get_rs_value(row):
        for rs in ['RS4', 'RS3', 'RS2', 'RS1']:
            if pd.notnull(row.get(rs)):
                return row[rs]
        return None
    df['RS_value'] = df.apply(get_rs_value, axis=1)
    df = df.dropna(subset=['RS_value'])
    if df.empty:
        logging.error("No RS values found after filtering")
        return
    # Rank the stocks based on RS_value
    df['rank'] = df['RS_value'].rank(method='min', ascending=False)
    total_stocks = len(df)
    df['rs_score'] = df['rank'].apply(lambda x: max(1, min(99, round((1 - (x - 1) / (total_stocks - 1)) * 100))))
    # Prepare bulk operations
    bulk_operations = []
    for _, row in df.iterrows():
        ticker = row['ticker']
        rs_score = row['rs_score']
        bulk_operations.append(UpdateOne(
            {"ticker": ticker, "date": latest_date},
            {"$set": {"rs_score": rs_score}},
                upsert=True
        ))
        logging.info(f"Ticker: {ticker}, RS Score: {rs_score}")
    # Perform bulk update
    if bulk_operations:
        ohlcv_collection.bulk_write(bulk_operations, ordered=False)
        logging.info("RS ranking and score calculation completed.")

# Run the daily cron job
def run_daily_cron_job():
    logging.info("Starting daily cron job...")
    
    # Step 1: Fetch today's OHLCV data for all tickers
    fetch_daily_ohlcv_data()
    
    # Step 2: Calculate RS values and daily percentage change
    calculate_rs_values()
    
    # Step 3: Calculate RS scores for all tickers
    calculate_rs_ranking()
    
    logging.info("Daily cron job completed.")

if __name__ == "__main__":
    run_daily_cron_job()
