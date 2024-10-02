import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import numpy as np

# MongoDB connection (hardcoded)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# Test tickers
tickers = ['TXN', 'STM', 'KGS', 'MPC', 'CMRE', 'MASI', 'OXY', 'TSLA', 'BABA']

# Benchmark ticker for S&P 500 (^GSPC)
benchmark_ticker = '^GSPC'

# Fetch 2 years of benchmark data from Yahoo Finance and store in MongoDB
def fetch_and_store_benchmark_data(ticker):
    print(f"Fetching 2 years of benchmark data for {ticker}")
    stock = yf.Ticker(ticker)
    hist = stock.history(period="2y")  # Fetch 2 years of data
    
    if not hist.empty:
        # Convert the data to MongoDB format
        for date, row in hist.iterrows():
            data = {
                "ticker": ticker,
                "date": date.to_pydatetime(),
                "open": row['Open'],
                "high": row['High'],
                "low": row['Low'],
                "close": row['Close'],
                "volume": row['Volume']
            }
            # Store the data in the ohlcv_data collection
            ohlcv_collection.update_one(
                {"ticker": ticker, "date": data["date"]},
                {"$set": data},
                upsert=True
            )
        print(f"Benchmark data for {ticker} stored successfully.")
    else:
        print(f"No data found for {ticker}.")

# Fetch and store 2 years of benchmark data (only once)
fetch_and_store_benchmark_data(benchmark_ticker)

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
    return rs_score

# Iterate over tickers and calculate RS
for ticker in tickers:
    print(f"Processing ticker: {ticker}")

    # Fetch ticker data from MongoDB
    ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))
    
    if len(ticker_data) > 0:
        # Convert to pandas DataFrame
        ticker_df = pd.DataFrame(ticker_data)

        # Calculate RS score
        rs_score = calculate_rs_score(ticker_df, benchmark_df)
        
        # Store RS score in the indicators collection
        indicator_data = {
            "ticker": ticker,
            "rs_score": rs_score,
            "date": pd.to_datetime('today')  # Store the current date
        }

        # Insert into indicators collection
        indicators_collection.update_one(
            {"ticker": ticker},
            {"$set": indicator_data},
            upsert=True
        )

        print(f"Stored RS score for {ticker}: {rs_score}")
    else:
        print(f"No data found for {ticker}")

print("Relative strength score calculation complete.")
