import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# List of tickers to process
tickers = ohlcv_collection.distinct('ticker')

# Function to fetch and update daily OHLCV data
def fetch_daily_ohlcv_data():
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        today_data = stock.history(period="5d")  # Fetch the last 5 days of data

        if not today_data.empty:
            date = today_data.index[-1].to_pydatetime()
            row = today_data.iloc[-1]

            # Create the data document
            data = {
                "ticker": ticker,
                "date": date,
                "open": row['Open'],
                "high": row['High'],
                "low": row['Low'],
                "close": row['Close'],
                "volume": row['Volume'],
            }

            # Use update_one with upsert=True to avoid duplicates
            ohlcv_collection.update_one(
                {"ticker": ticker, "date": date},
                {"$set": data},
                upsert=True
            )
            
            logging.info(f"Upserted record for {ticker} on {date}")
    
    logging.info("Daily OHLCV data updated successfully.")

# Function to calculate RS values (RS1, RS2, RS3, RS4) and daily percentage change
def calculate_rs_values():
    periods = {
        "RS1": 63,
        "RS2": 126,
        "RS3": 189,
        "RS4": 252
    }

    for ticker in tickers:
        # Fetch historical data for the ticker
        history = list(ohlcv_collection.find({"ticker": ticker}).sort("date", -1).limit(252))
        if len(history) < 252:
            logging.warning(f"Not enough data to calculate RS for ticker {ticker}")
            continue

        history_df = pd.DataFrame(history)
        history_df['daily_pct_change'] = history_df['close'].pct_change() * 100

        # Calculate rolling returns for RS1, RS2, RS3, RS4
        updates = {}
        for rs_key, period in periods.items():
            if len(history_df) >= period:
                rolling_return = (history_df['close'].iloc[0] - history_df['close'].iloc[period]) / history_df['close'].iloc[period] * 100
                updates[rs_key] = rolling_return

        # Add daily percentage change to updates
        updates["daily_pct_change"] = history_df['daily_pct_change'].iloc[0]

        # Update all RS values and daily percentage change in a single operation
        if updates:
            ohlcv_collection.update_one(
                {"ticker": ticker, "date": history_df['date'].iloc[0]},
                {"$set": updates},
                upsert=True
            )
    
    logging.info("RS values and daily percentage change calculated.")

# Function to calculate and rank RS score using RS4, RS3, RS2, RS1 as fallbacks
def calculate_rs_ranking():
    # Find the latest date with RS values
    latest_date = ohlcv_collection.find_one(
        {"$or": [
            {"RS4": {"$exists": True, "$ne": None}},
            {"RS3": {"$exists": True, "$ne": None}},
            {"RS2": {"$exists": True, "$ne": None}},
            {"RS1": {"$exists": True, "$ne": None}},
        ]},
        sort=[("date", -1)],
        projection={"date": 1}
    )
    
    if not latest_date:
        logging.error("No records found with RS values")
        return
        
    latest_date = latest_date["date"]
    logging.info(f"Latest trading date for RS values: {latest_date}")

    # Get all stocks with RS values for the latest date
    stocks = list(ohlcv_collection.find(
        {"date": latest_date, "$or": [
            {"RS4": {"$exists": True, "$ne": None}},
            {"RS3": {"$exists": True, "$ne": None}},
            {"RS2": {"$exists": True, "$ne": None}},
            {"RS1": {"$exists": True, "$ne": None}},
        ]},
        {"ticker": 1, "RS4": 1, "RS3": 1, "RS2": 1, "RS1": 1}
    ))

    # Sort stocks by RS value (using fallbacks)
    stocks.sort(key=lambda x: (
        x.get('RS4') or 
        x.get('RS3') or 
        x.get('RS2') or 
        x.get('RS1') or 
        float('-inf')
    ))

    total_stocks = len(stocks)
    if total_stocks == 0:
        logging.error("No stocks found with RS values")
        return

    # Calculate and update RS scores
    bulk_operations = []
    for rank, doc in enumerate(stocks, 1):
        ticker = doc['ticker']
        percentile_rank = (rank / total_stocks) * 100
        rs_score = max(1, min(99, round(percentile_rank)))

        bulk_operations.append(UpdateOne(
            {"ticker": ticker, "date": latest_date},
            {"$set": {"rs_score": rs_score}},
            upsert=True
        ))

        logging.info(f"Ticker: {ticker}, Rank: {rank}, RS Score: {rs_score}")

    # Execute bulk update
    if bulk_operations:
        indicators_collection.bulk_write(bulk_operations, ordered=False)

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