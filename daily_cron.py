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

# Benchmark ticker for S&P 500 (^GSPC)
benchmark_ticker = '^GSPC'

# Function to normalize RS score to 1-99 range
def normalize_rs_score(rs_raw, max_score, min_score):
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

# Function to calculate RS score and check for new RS highs
def calculate_and_store_relative_strength(ticker_data, benchmark_data):
    # Define periods and weights
    periods = [63, 126, 189, 252]
    weights = [2, 1, 1, 1]
    rs_values = []

    for period in periods:
        if len(ticker_data) >= period and len(benchmark_data) >= period:
            rs_value = (ticker_data['close'].iloc[-1] / ticker_data['close'].iloc[-period]) - (
                        benchmark_data['close'].iloc[-1] / benchmark_data['close'].iloc[-period])
            rs_values.append(rs_value)
        else:
            rs_values.append(0.0)

    # Calculate raw RS score
    rs_raw = sum([rs_values[i] * weights[i] for i in range(4)])
    max_score = sum(weights)
    min_score = -max_score
    rs_score = normalize_rs_score(rs_raw, max_score, min_score)

    # Check for RS New Highs over a 40-day lookback period
    lookback = 40
    rs_high = max(ticker_data['rs_score'].tail(lookback)) if 'rs_score' in ticker_data else 0
    new_rs_high = rs_score > rs_high

    return rs_score, new_rs_high

# Function to fetch and store OHLCV data from Yahoo Finance
def fetch_and_store_ohlcv(ticker):
    print(f"Fetching daily OHLCV data for {ticker}")
    stock = yf.Ticker(ticker)
    hist = stock.history(period="1d")  # Fetch only 1 day of data

    if not hist.empty:
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
            ohlcv_collection.update_one(
                {"ticker": ticker, "date": data["date"]},
                {"$set": data},
                upsert=True
            )
        print(f"Daily OHLCV data for {ticker} stored successfully.")
    else:
        print(f"No data found for {ticker}.")

# Function to process RS for all tickers
def process_all_tickers():
    tickers = ohlcv_collection.distinct("ticker")
    benchmark_data = pd.DataFrame(list(ohlcv_collection.find({"ticker": benchmark_ticker}).sort("date", 1)))

    for ticker in tickers:
        print(f"Processing RS for ticker: {ticker}")
        ticker_data = pd.DataFrame(list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1)))

        if not ticker_data.empty and not benchmark_data.empty:
            rs_score, new_rs_high = calculate_and_store_relative_strength(ticker_data, benchmark_data)

            indicator_data = {
                "ticker": ticker,
                "rs_score": rs_score,
                "new_rs_high": new_rs_high,
                "date": datetime.utcnow()
            }

            indicators_collection.update_one(
                {"ticker": ticker},
                {"$set": indicator_data},
                upsert=True
            )

            print(f"Stored RS score for {ticker}: {rs_score}, New RS High: {new_rs_high}")

if __name__ == "__main__":
    # Example tickers
    example_tickers = ['AAPL', 'TSLA', 'MSFT']

    # Fetch and store daily OHLCV data for example tickers
    for ticker in example_tickers:
        fetch_and_store_ohlcv(ticker)

    # Process RS for all tickers
    process_all_tickers()

    print("Daily cron job completed.")
