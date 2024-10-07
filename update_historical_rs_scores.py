import yfinance as yf
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import numpy as np

# MongoDB connection (hardcoded)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

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
            previous_ticker_close = merged_df['close_ticker'].iloc[-(n + 1)]
            current_benchmark_close = merged_df['close_benchmark'].iloc[-1]
            previous_benchmark_close = merged_df['close_benchmark'].iloc[-(n + 1)]

            rs_value = (current_ticker_close / previous_ticker_close) - (current_benchmark_close / previous_benchmark_close)
            rs_values.append(rs_value)
        else:
            rs_values.append(0.0)

    rs_raw = sum([rs_values[i] * weights[i] for i in range(len(rs_values))])
    max_score = sum(weights)
    min_score = -max_score
    rs_score = normalize_rs_score(rs_raw, max_score, min_score)
    rs_score = max(1, min(99, rs_score))

    return rs_score

# Function to process historical RS scores for each ticker
def update_historical_rs_scores():
    tickers = ohlcv_collection.distinct('ticker')

    # Load benchmark data from MongoDB
    benchmark_data = list(ohlcv_collection.find({"ticker": benchmark_ticker}).sort("date", 1))
    benchmark_df = pd.DataFrame(benchmark_data)
    benchmark_df['date'] = pd.to_datetime(benchmark_df['date'])

    for ticker in tickers:
        print(f"Processing historical RS scores for ticker: {ticker}")
        ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))

        if len(ticker_data) > 0:
            ticker_df = pd.DataFrame(ticker_data)
            ticker_df['date'] = pd.to_datetime(ticker_df['date'])

            # Merge on 'date' to align the data
            merged_df = pd.merge(ticker_df[['date', 'close']], benchmark_df[['date', 'close']], on='date', suffixes=('_ticker', '_benchmark'))
            merged_df = merged_df.sort_values('date').reset_index(drop=True)

            for i in range(len(merged_df)):
                if i < 252:
                    continue  # Skip the first 252 days as we don't have enough data for all periods

                # Calculate RS score for the current date
                rs_score = calculate_rs_score(merged_df.iloc[:i+1])

                # Get the date
                date = merged_df['date'].iloc[i]

                # Update or insert the RS score in MongoDB
                ohlcv_collection.update_one(
                    {"ticker": ticker, "date": date},
                    {"$set": {"rs_score": rs_score}},
                    upsert=True
                )

                print(f"Updated RS score for {ticker} on {date}: RS Score={rs_score}")

        else:
            print(f"No data found for ticker: {ticker}")

if __name__ == "__main__":
    update_historical_rs_scores()
