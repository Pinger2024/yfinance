import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime

# MongoDB connection (hardcoded)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# List of tickers
tickers = ohlcv_collection.distinct('ticker')

# Benchmark ticker for S&P 500 (^GSPC)
benchmark_ticker = '^GSPC'

# Fetch today's data for all tickers and store it in MongoDB
def fetch_daily_ohlcv_data():
    for ticker in tickers:
        print(f"Fetching daily OHLCV data for {ticker}")
        stock = yf.Ticker(ticker)
        today_data = stock.history(period="1d")

        if not today_data.empty:
            date = today_data.index[0].to_pydatetime()
            row = today_data.iloc[0]
            data = {
                "ticker": ticker,
                "date": date,
                "open": row['Open'],
                "high": row['High'],
                "low": row['Low'],
                "close": row['Close'],
                "volume": row['Volume']
            }

            # Store today's data in the collection
            ohlcv_collection.update_one(
                {"ticker": ticker, "date": date},
                {"$set": data},
                upsert=True
            )
            print(f"Stored data for {ticker} on {date}")
        else:
            print(f"No data found for {ticker} on {datetime.now()}")

# Function to calculate and store relative strength (RS) scores
def calculate_and_store_relative_strength():
    # Load benchmark data from MongoDB
    benchmark_data = list(ohlcv_collection.find({"ticker": benchmark_ticker}).sort("date", 1))
    benchmark_df = pd.DataFrame(benchmark_data)

    # Function to normalize RS score to 1-99 range
    def normalize_rs_score(rs_raw, max_score, min_score):
        return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

    # Function to calculate RS score based on multiple periods
    def calculate_rs_score(ticker_data, benchmark_data):
        periods = [63, 126, 189, 252]
        weights = [2, 1, 1, 1]
        rs_values = []

        for period in periods:
            if len(ticker_data) >= period and len(benchmark_data) >= period:
                rs_value = (ticker_data['close'].iloc[-1] / ticker_data['close'].iloc[-period]) - \
                           (benchmark_data['close'].iloc[-1] / benchmark_data['close'].iloc[-period])
                rs_values.append(rs_value)
            else:
                rs_values.append(0.0)

        rs_raw = sum([rs_values[i] * weights[i] for i in range(4)])
        max_score = sum(weights)
        min_score = -max_score
        return normalize_rs_score(rs_raw, max_score, min_score)

    # Iterate over all tickers and calculate RS scores
    for ticker in tickers:
        print(f"Processing RS for ticker: {ticker}")
        ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))

        if len(ticker_data) > 0:
            ticker_df = pd.DataFrame(ticker_data)
            rs_score = calculate_rs_score(ticker_df, benchmark_df)

            # Detect new RS highs
            prev_rs_high = indicators_collection.find_one({"ticker": ticker}, {"new_rs_high": 1})
            if prev_rs_high:
                new_rs_high = rs_score > prev_rs_high.get('rs_score', -np.inf)
            else:
                new_rs_high = False

            # Store RS score and new RS high status
            indicator_data = {
                "ticker": ticker,
                "rs_score": min(max(rs_score, 1), 99),  # Ensure RS score is between 1 and 99
                "new_rs_high": new_rs_high,
                "date": pd.to_datetime('today')
            }

            indicators_collection.update_one(
                {"ticker": ticker},
                {"$set": indicator_data},
                upsert=True
            )
            print(f"Stored RS score for {ticker}: {rs_score}")
        else:
            print(f"No data found for ticker: {ticker}")

# Run the daily data fetch and RS calculation
def run_daily_cron_job():
    print("Starting daily cron job...")
    fetch_daily_ohlcv_data()
    calculate_and_store_relative_strength()
    print("Daily cron job completed.")

if __name__ == "__main__":
    run_daily_cron_job()
