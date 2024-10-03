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

# Fetch the latest available data for all tickers and store it in MongoDB
def fetch_daily_ohlcv_data():
    for ticker in tickers:
        print(f"Fetching daily OHLCV data for {ticker}")
        stock = yf.Ticker(ticker)
        today_data = stock.history(period="5d")  # Fetch the last 5 days of data

        if not today_data.empty:
            date = today_data.index[-1].to_pydatetime()  # Get the latest available date
            row = today_data.iloc[-1]
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

# Function to detect new RS highs based on RS line
def check_new_rs_high(merged_df, lookback=40):
    if len(merged_df) >= lookback + 1:
        # Calculate RS line
        merged_df['rs_line'] = merged_df['close_ticker'] / merged_df['close_benchmark']
        current_rs = merged_df['rs_line'].iloc[-1]
        rs_line_lookback = merged_df['rs_line'].iloc[-(lookback+1):-1]

        rs_high = rs_line_lookback.max()

        if current_rs > rs_high:
            return True
    return False

# Function to calculate and store RS scores and detect RS new highs
def calculate_and_store_relative_strength():
    # Load benchmark data from MongoDB
    benchmark_data = list(ohlcv_collection.find({"ticker": benchmark_ticker}).sort("date", 1))
    benchmark_df = pd.DataFrame(benchmark_data)
    benchmark_df['date'] = pd.to_datetime(benchmark_df['date'])

    # Iterate over all tickers and calculate RS scores
    for ticker in tickers:
        print(f"Processing RS for ticker: {ticker}")
        ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))

        if len(ticker_data) > 0:
            ticker_df = pd.DataFrame(ticker_data)
            ticker_df['date'] = pd.to_datetime(ticker_df['date'])

            # Merge on 'date' to align the data
            merged_df = pd.merge(ticker_df[['date', 'close']], benchmark_df[['date', 'close']], on='date', suffixes=('_ticker', '_benchmark'))
            merged_df = merged_df.sort_values('date').reset_index(drop=True)

            if len(merged_df) >= 1:
                rs_score = calculate_rs_score(merged_df)

                # Detect new RS highs
                new_rs_high = check_new_rs_high(merged_df)

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
                print(f"Stored RS score for {ticker}: {rs_score}, New RS High: {new_rs_high}")
            else:
                print(f"No merged data available for ticker: {ticker}")
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
