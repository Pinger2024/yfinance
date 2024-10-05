import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime

# MongoDB connection (hardcoded)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

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

# Function to calculate RS score for each day and update OHLCV data
def update_ohlcv_with_rs_scores():
    # Load benchmark data from MongoDB
    benchmark_data = list(ohlcv_collection.find({"ticker": benchmark_ticker}).sort("date", 1))
    benchmark_df = pd.DataFrame(benchmark_data)
    benchmark_df['date'] = pd.to_datetime(benchmark_df['date'])

    # List of tickers
    tickers = ohlcv_collection.distinct('ticker')

    # Iterate over all tickers and calculate RS scores
    for ticker in tickers:
        print(f"Processing RS scores for ticker: {ticker}")
        ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))

        if len(ticker_data) > 0:
            ticker_df = pd.DataFrame(ticker_data)
            ticker_df['date'] = pd.to_datetime(ticker_df['date'])

            # Merge on 'date' to align the data
            merged_df = pd.merge(ticker_df[['date', 'close']], benchmark_df[['date', 'close']], on='date', suffixes=('_ticker', '_benchmark'))
            merged_df = merged_df.sort_values('date').reset_index(drop=True)

            # Calculate and update RS score for each day
            for idx in range(len(merged_df)):
                subset_df = merged_df.iloc[:idx + 1]  # Use data up to the current day
                rs_score = calculate_rs_score(subset_df)

                # Update the OHLCV collection with the RS score for the current date
                ohlcv_collection.update_one(
                    {"ticker": ticker, "date": merged_df['date'].iloc[idx]},
                    {"$set": {"rs_score": float(rs_score)}}
                )
                print(f"Updated RS score for {ticker} on {merged_df['date'].iloc[idx]}: {rs_score}")

        else:
            print(f"No data found for ticker: {ticker}")

# Run the RS score update process
if __name__ == "__main__":
    update_ohlcv_with_rs_scores()
