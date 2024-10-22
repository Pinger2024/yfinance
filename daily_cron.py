import yfinance as yf
from pymongo import MongoClient, UpdateOne
import pandas as pd
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
        history = list(ohlcv_collection.find({"ticker": ticker}).sort("date", -1).limit(252))
        if len(history) < 252:
            logging.warning(f"Not enough data to calculate RS for ticker {ticker}")
            continue

        history_df = pd.DataFrame(history)
        history_df['daily_pct_change'] = history_df['close'].pct_change() * 100

        updates = {}
        for rs_key, period in periods.items():
            if len(history_df) >= period:
                rolling_return = (history_df['close'].iloc[0] - history_df['close'].iloc[period]) / history_df['close'].iloc[period] * 100
                updates[rs_key] = rolling_return

        updates["daily_pct_change"] = history_df['daily_pct_change'].iloc[0]

        if updates:
            ohlcv_collection.update_one(
                {"ticker": ticker, "date": history_df['date'].iloc[0]},
                {"$set": updates},
                upsert=True
            )
    
    logging.info("RS values and daily percentage change calculated.")

# Function to calculate weighted RS score for a stock
def calculate_weighted_rs_score(ticker):
    try:
        latest_date = ohlcv_collection.find_one(
            {"ticker": ticker, "$or": [
                {"RS4": {"$exists": True, "$ne": None}},
                {"RS3": {"$exists": True, "$ne": None}},
                {"RS2": {"$exists": True, "$ne": None}},
                {"RS1": {"$exists": True, "$ne": None}},
            ]},
            sort=[("date", -1)],
            projection={"date": 1}
        )["date"]

        rs_data = ohlcv_collection.find_one(
            {"ticker": ticker, "date": latest_date},
            {"RS1": 1, "RS2": 1, "RS3": 1, "RS4": 1}
        )

        weights = {
            "RS1": 0.40,
            "RS2": 0.30,
            "RS3": 0.20,
            "RS4": 0.10
        }

        weighted_score = sum(
            weights[f"RS{i}"] * rs_data.get(f"RS{i}", 0)
            for i in range(1, 5)
        )

        return {
            "ticker": ticker,
            "date": latest_date,
            "weighted_score": weighted_score
        }

    except Exception as e:
        logging.error(f"Error calculating RS score for {ticker}: {str(e)}")
        return None

# Normalize and update RS scores in the indicators collection
def normalize_and_update_rs_scores():
    scores = []
    for ticker in tickers:
        score = calculate_weighted_rs_score(ticker)
        if score:
            scores.append(score)

    scores_df = pd.DataFrame(scores)
    scores_df["rank"] = scores_df["weighted_score"].rank(pct=True)
    scores_df["rs_score"] = (scores_df["rank"] * 98 + 1).round().astype(int)

    bulk_operations = []
    for _, row in scores_df.iterrows():
        bulk_operations.append(UpdateOne(
            {"ticker": row["ticker"], "date": row["date"]},
            {"$set": {"rs_score": row["rs_score"]}},
            upsert=True
        ))

    if bulk_operations:
        indicators_collection.bulk_write(bulk_operations, ordered=False)
        logging.info(f"Updated {len(bulk_operations)} RS scores")

# Main function to run the daily cron job
def run_daily_cron_job():
    logging.info("Starting daily cron job...")

    # Step 1: Fetch today's OHLCV data for all tickers
    fetch_daily_ohlcv_data()

    # Step 2: Calculate RS values and daily percentage change
    calculate_rs_values()

    # Step 3: Calculate RS scores for all tickers
    normalize_and_update_rs_scores()

    logging.info("Daily cron job completed.")

if __name__ == "__main__":
    run_daily_cron_job()
