import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import warnings

# Suppress specific warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Setup logging to provide more detailed information
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# List of tickers to process
tickers = ohlcv_collection.distinct('ticker')

# Fetch and update daily OHLCV data
def fetch_daily_ohlcv_data():
    logging.info("Fetching daily OHLCV data for tickers...")
    for ticker in tickers:
        logging.info(f"Processing ticker: {ticker}")
        stock = yf.Ticker(ticker)
        try:
            today_data = stock.history(period="5d")  # Fetch the last 5 days of data
            if not today_data.empty:
                date = today_data.index[-1].to_pydatetime()
                row = today_data.iloc[-1]

                # Store today's data in the ohlcv_data collection
                data = {
                    "ticker": ticker,
                    "date": date,
                    "open": row['Open'],
                    "high": row['High'],
                    "low": row['Low'],
                    "close": row['Close'],
                    "volume": row['Volume'],
                }

                ohlcv_collection.update_one(
                    {"ticker": ticker, "date": date},
                    {"$set": data},
                    upsert=True
                )
                logging.info(f"Updated OHLCV data for ticker: {ticker}, date: {date}")
        except Exception as e:
            logging.error(f"Error fetching data for ticker {ticker}: {e}")

    logging.info("Daily OHLCV data update completed.")

# Function to calculate RS values (RS1, RS2, RS3, RS4) and daily percentage change
def calculate_rs_values():
    logging.info("Calculating RS values for all tickers...")
    periods = {
        "RS1": 63,
        "RS2": 126,
        "RS3": 189,
        "RS4": 252
    }

    for ticker in tickers:
        logging.info(f"Calculating RS values for ticker: {ticker}")
        try:
            # Fetch historical data for the ticker
            history = list(ohlcv_collection.find({"ticker": ticker}).sort("date", -1).limit(252))
            if len(history) < 252:
                logging.warning(f"Not enough data to calculate RS for ticker {ticker}")
                continue

            history_df = pd.DataFrame(history)
            history_df['daily_pct_change'] = history_df['close'].pct_change() * 100

            # Calculate rolling returns for RS1, RS2, RS3, RS4
            for rs_key, period in periods.items():
                if len(history_df) >= period:
                    rolling_return = (history_df['close'].iloc[0] - history_df['close'].iloc[period]) / history_df['close'].iloc[period] * 100
                    ohlcv_collection.update_one(
                        {"ticker": ticker, "date": history_df['date'].iloc[0]},
                        {"$set": {rs_key: rolling_return}},
                        upsert=True
                    )
                    logging.info(f"Updated {rs_key} for ticker: {ticker}")

            # Update daily percentage change
            daily_pct_change = history_df['daily_pct_change'].iloc[0]
            ohlcv_collection.update_one(
                {"ticker": ticker, "date": history_df['date'].iloc[0]},
                {"$set": {"daily_pct_change": daily_pct_change}},
                upsert=True
            )
            logging.info(f"Updated daily_pct_change for ticker: {ticker}")
        except Exception as e:
            logging.error(f"Error calculating RS values for ticker {ticker}: {e}")
    
    logging.info("RS values and daily percentage change calculation completed.")

# Function to calculate and rank RS score using RS4, RS3, RS2, RS1 as fallbacks
def calculate_rs_ranking():
    logging.info("Calculating RS rankings...")
    try:
        latest_date = ohlcv_collection.find_one(
            {"$or": [
                {"RS4": {"$exists": True, "$ne": None}},
                {"RS3": {"$exists": True, "$ne": None}},
                {"RS2": {"$exists": True, "$ne": None}},
                {"RS1": {"$exists": True, "$ne": None}},
            ]},
            sort=[("date", -1)],
            projection={"date": 1}
        )["date"]

        logging.info(f"Latest trading date for RS values: {latest_date}")

        cursor = ohlcv_collection.find(
            {"date": latest_date, "$or": [
                {"RS4": {"$exists": True, "$ne": None}},
                {"RS3": {"$exists": True, "$ne": None}},
                {"RS2": {"$exists": True, "$ne": None}},
                {"RS1": {"$exists": True, "$ne": None}},
            ]},
            {"ticker": 1, "RS4": 1, "RS3": 1, "RS2": 1, "RS1": 1}
        ).sort("RS4", 1)

        total_stocks = cursor.count()
        rank = 1

        for doc in cursor:
            ticker = doc['ticker']
            rs_value = (
                doc.get('RS4') or
                doc.get('RS3') or
                doc.get('RS2') or
                doc.get('RS1')
            )

            if rs_value is None:
                continue  # Skip if no RS value is found

            # Calculate percentile rank (convert rank to 1-99 scale)
            percentile_rank = (rank / total_stocks) * 100
            rs_score = max(1, min(99, round(percentile_rank)))  # Ensure it's between 1 and 99

            # Log ticker rank info
            logging.info(f"Ticker: {ticker}, RS value: {rs_value}, Rank: {rank}, RS Score: {rs_score}")

            # Update the indicators collection with the new RS score
            indicators_collection.update_one(
                {"ticker": ticker, "date": latest_date},
                {"$set": {"rs_score": rs_score}},
                upsert=True
            )

            rank += 1

        logging.info("RS ranking and score calculation completed.")
    except Exception as e:
        logging.error(f"Error during RS ranking calculation: {e}")

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
