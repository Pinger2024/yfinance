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
sector_trends_collection = db['sector_trends']

# List of tickers to process
tickers = ohlcv_collection.distinct('ticker')

# Benchmark ticker for S&P 500 (^GSPC)
benchmark_ticker = '^GSPC'

# Function to normalize RS score to 1-99 range
def normalize_rs_score(rs_raw, max_score, min_score):
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

# Function to calculate RS score based on multiple periods
def calculate_rs_score(merged_df):
    periods = [63, 126, 189, 252]
    weights = [2, 1, 1, 1]
    rs_values = []

    for i, period in enumerate(periods):
        n = min(len(merged_df) - 1, period)
        if n > 0:
            current_ticker_close = merged_df['close_ticker'].iloc[-1]
            previous_ticker_close = merged_df['close_ticker'].iloc[-(n+1)]
            current_benchmark_close = merged_df['close_benchmark'].iloc[-1]
            previous_benchmark_close = merged_df['close_benchmark'].iloc[-(n+1)]

            rs_value = (current_ticker_close / previous_ticker_close) - \
                       (current_benchmark_close / previous_benchmark_close)
            rs_values.append(rs_value)
        else:
            rs_values.append(0.0)

    rs_raw = sum([rs_values[i] * weights[i] for i in range(len(rs_values))])
    max_score = sum(weights)
    min_score = -max_score
    rs_score = normalize_rs_score(rs_raw, max_score, min_score)
    rs_score = max(1, min(99, rs_score))

    return rs_score

# Fetch and update daily OHLCV data
def fetch_daily_ohlcv_data():
    for ticker in tickers:
        stock = yf.Ticker(ticker)
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

# Calculate and store RS scores in both collections
def calculate_and_store_rs_scores():
    # Load benchmark data from MongoDB
    benchmark_data = list(ohlcv_collection.find({"ticker": benchmark_ticker}).sort("date", 1))
    benchmark_df = pd.DataFrame(benchmark_data)
    benchmark_df['date'] = pd.to_datetime(benchmark_df['date'])

    # Iterate over all tickers and calculate RS scores
    for ticker in tickers:
        ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))

        if len(ticker_data) > 0:
            ticker_df = pd.DataFrame(ticker_data)
            ticker_df['date'] = pd.to_datetime(ticker_df['date'])

            # Merge on 'date' to align the data
            merged_df = pd.merge(ticker_df[['date', 'close']], benchmark_df[['date', 'close']], on='date', suffixes=('_ticker', '_benchmark'))
            merged_df = merged_df.sort_values('date').reset_index(drop=True)

            if len(merged_df) >= 1:
                rs_score = calculate_rs_score(merged_df)

                # Update ohlcv_data collection with RS score
                ohlcv_collection.update_many(
                    {"ticker": ticker, "date": {"$gte": merged_df['date'].min(), "$lte": merged_df['date'].max()}},
                    {"$set": {"rs_score": rs_score}}
                )

                # Store RS score in the indicators collection
                latest_data = ticker_df.iloc[-1]
                indicator_data = {
                    "ticker": ticker,
                    "rs_score": rs_score,
                    "date": latest_data['date'],
                    # You can add other indicator fields as needed
                }

                indicators_collection.update_one(
                    {"ticker": ticker},
                    {"$set": indicator_data},
                    upsert=True
                )

                logging.info(f"Stored RS score for {ticker}: {rs_score}")

# Calculate sector and industry trends
def calculate_sector_and_industry_trends():
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')

    # Aggregate sector data
    pipeline_sector = [
        {"$match": {"date": today, "sector": {"$exists": True, "$ne": None}}},
        {"$group": {
            "_id": "$sector",
            "average_rs": {"$avg": "$rs_score"},
            "tickers_in_sector": {"$addToSet": "$ticker"}
        }}
    ]
    sector_data = list(ohlcv_collection.aggregate(pipeline_sector))

    # Aggregate industry data
    pipeline_industry = [
        {"$match": {"date": today, "industry": {"$exists": True, "$ne": None}}},
        {"$group": {
            "_id": "$industry",
            "average_rs": {"$avg": "$rs_score"},
            "tickers_in_industry": {"$addToSet": "$ticker"}
        }}
    ]
    industry_data = list(ohlcv_collection.aggregate(pipeline_industry))

    # Insert or update sector and industry data in sector_trends_collection
    for sector in sector_data:
        sector_trend = {
            "date": today,
            "sector": sector["_id"],
            "average_rs": sector["average_rs"],
            "tickers_in_sector": sector["tickers_in_sector"],
            "type": "sector"
        }
        sector_trends_collection.update_one(
            {"date": today, "sector": sector["_id"], "type": "sector"},
            {"$set": sector_trend},
            upsert=True
        )
        logging.info(f"Stored sector data for {sector['_id']} on {today}")

    for industry in industry_data:
        industry_trend = {
            "date": today,
            "industry": industry["_id"],
            "average_rs": industry["average_rs"],
            "tickers_in_industry": industry["tickers_in_industry"],
            "type": "industry"
        }
        sector_trends_collection.update_one(
            {"date": today, "industry": industry["_id"], "type": "industry"},
            {"$set": industry_trend},
            upsert=True
        )
        logging.info(f"Stored industry data for {industry['_id']} on {today}")

# Run the daily cron job
def run_daily_cron_job():
    logging.info("Starting daily cron job...")
    
    # Step 1: Fetch today's OHLCV data for all tickers
    fetch_daily_ohlcv_data()
    
    # Step 2: Calculate RS scores for all tickers
    calculate_and_store_rs_scores()

    # Step 3: Calculate sector and industry trends
    calculate_sector_and_industry_trends()
    
    logging.info("Daily cron job completed.")

if __name__ == "__main__":
    run_daily_cron_job()
