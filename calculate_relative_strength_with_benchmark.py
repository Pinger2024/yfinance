import yfinance as yf
from pymongo import MongoClient
import pandas as pd
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

# Convert benchmark data to a pandas DataFrame
benchmark_df = pd.DataFrame(benchmark_data)

# Function to normalize RS score to 1-99 range
def normalize_rs_score(rs_raw, max_score, min_score):
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

# Function to calculate relative strength (RS) score based on multiple periods
def calculate_rs_score(ticker_data, benchmark_data):
    # Define periods and weights
    periods = [63, 126, 189, 252]  # Corresponds to 3 months, 6 months, etc.
    weights = [2, 1, 1, 1]  # Weights for each period, as per the Pine Script

    rs_values = []
    for period in periods:
        if len(ticker_data) >= period and len(benchmark_data) >= period:
            # Calculate RS over this period
            rs_value = (ticker_data['close'].iloc[-1] / ticker_data['close'].iloc[-period]) - (benchmark_data['close'].iloc[-1] / benchmark_data['close'].iloc[-period])
            rs_values.append(rs_value)
        else:
            rs_values.append(0.0)  # Default value if not enough data

    # Calculate raw RS score
    rs_raw = sum([rs_values[i] * weights[i] for i in range(4)])

    # Calculate max and min scores for normalization
    max_score = sum(weights)
    min_score = -max_score

    # Normalize RS score to a range between 1-99
    rs_score = normalize_rs_score(rs_raw, max_score, min_score)
    
    # Ensure the RS score is within bounds 1 to 99
    rs_score = max(1, min(99, rs_score))
    
    return rs_score

# Function to detect new RS highs
def detect_new_rs_high(ticker, ticker_data, benchmark_data):
    # Calculate RS line (current RS line = close / benchmark close)
    rs_line = ticker_data['close'].iloc[-1] / benchmark_data['close'].iloc[-1]
    
    # Check if this is a new high
    if len(ticker_data) >= 252:  # Ensure we have enough data to compare
        rs_high = max(ticker_data['close'] / benchmark_data['close'])
        if rs_line > rs_high:
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

        # Calculate RS score
        rs_score = calculate_rs_score(ticker_df, benchmark_df)
        
        # Detect new RS high
        new_rs_high = detect_new_rs_high(ticker, ticker_df, benchmark_df)
        
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
        print(f"No data found for {ticker}")

print("Relative strength score calculation complete.")
