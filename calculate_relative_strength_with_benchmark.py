import pandas as pd
from pymongo import MongoClient
import numpy as np

# MongoDB connection (hardcoded)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# Benchmark ticker for S&P 500 (^GSPC)
benchmark_ticker = '^GSPC'

# Load benchmark data from MongoDB
benchmark_data = list(ohlcv_collection.find({"ticker": benchmark_ticker}).sort("date", 1))
benchmark_df = pd.DataFrame(benchmark_data)
benchmark_df['date'] = pd.to_datetime(benchmark_df['date'])

# Function to normalize RS score to 1-99 range
def normalize_rs_score(rs_raw, max_score, min_score):
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

# Function to calculate relative strength (RS) score based on multiple periods
def calculate_rs_score(merged_df):
    # Define periods and weights
    periods = [63, 126, 189, 252]  # Corresponds to ~3 months, 6 months, etc.
    weights = [2, 1, 1, 1]  # Weights for each period, as per the Pine Script

    rs_values = []
    for period in periods:
        if len(merged_df) >= period + 1:
            # Calculate RS over this period
            current_ticker_close = merged_df['close_ticker'].iloc[-1]
            previous_ticker_close = merged_df['close_ticker'].iloc[-period-1]
            current_benchmark_close = merged_df['close_benchmark'].iloc[-1]
            previous_benchmark_close = merged_df['close_benchmark'].iloc[-period-1]

            rs_value = (current_ticker_close / previous_ticker_close) - (current_benchmark_close / previous_benchmark_close)
            rs_values.append(rs_value)
        else:
            rs_values.append(0.0)  # Default value if not enough data

    # Calculate raw RS score
    rs_raw = sum([rs_values[i] * weights[i] for i in range(len(periods))])

    # Calculate max and min scores for normalization
    max_score = sum(weights)
    min_score = -max_score

    # Normalize RS score to a range between 1-99
    rs_score = normalize_rs_score(rs_raw, max_score, min_score)
    
    # Ensure the RS score is within bounds 1 to 99
    rs_score = max(1, min(99, rs_score))
    
    return rs_score

# Function to detect new RS highs
def detect_new_rs_high(merged_df, lookback=40):
    # Ensure we have enough data
    if len(merged_df) >= lookback + 1:
        # Calculate RS line
        merged_df['rs_line'] = merged_df['close_ticker'] / merged_df['close_benchmark']
        rs_line = merged_df['rs_line']
        # Get RS line for the lookback period excluding the current value
        rs_line_lookback = rs_line.iloc[-lookback-1:-1]
        current_rs = rs_line.iloc[-1]
        rs_high = rs_line_lookback.max()
        # Check if current RS Line is greater than previous highs
        if current_rs > rs_high:
            return True
    return False

# Fetch all unique tickers from the MongoDB database
tickers = ohlcv_collection.distinct("ticker")

# Iterate over all tickers and calculate RS
for ticker in tickers:
    print(f"Processing ticker: {ticker}")

    # Fetch ticker data from MongoDB
    ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))
    
    if len(ticker_data) > 0:
        # Convert to pandas DataFrame
        ticker_df = pd.DataFrame(ticker_data)
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])

        # Merge on date
        merged_df = pd.merge(ticker_df[['date', 'close']], benchmark_df[['date', 'close']], on='date', suffixes=('_ticker', '_benchmark'), how='inner')

        # Ensure the data is sorted by date
        merged_df = merged_df.sort_values('date').reset_index(drop=True)

        if len(merged_df) >= 1:
            # Calculate RS score
            rs_score = calculate_rs_score(merged_df)
            
            # Detect new RS high
            new_rs_high = detect_new_rs_high(merged_df)
            
            # Store RS score and whether it's a new RS high in the indicators collection
            indicator_data = {
                "ticker": ticker,
                "rs_score": rs_score,
                "new_rs_high": new_rs_high,
                "date": pd.to_datetime('today')  # Store the current date
            }

            # Insert into indicators collection
            indicators_collection.update_one(
                {"ticker": ticker},
                {"$set": indicator_data},
                upsert=True
            )

            print(f"Stored RS score for {ticker}: {rs_score}, New RS High: {new_rs_high}")
        else:
            print(f"No merged data available for {ticker}")
    else:
        print(f"No data found for {ticker}")

print("Relative strength score calculation complete.")
