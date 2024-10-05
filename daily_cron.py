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

            # Fetch sector information
            stock_info = stock.info
            sector = stock_info.get('sector', 'Unknown')

            data = {
                "ticker": ticker,
                "date": date,
                "open": row['Open'],
                "high": row['High'],
                "low": row['Low'],
                "close": row['Close'],
                "volume": row['Volume'],
                "sector": sector  # Store sector information
            }

            # Store today's data in the OHLCV collection
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

# Function to convert numpy data types to native Python types
def convert_numpy_types(data):
    if isinstance(data, dict):
        return {k: convert_numpy_types(v) for k, v in data.items()}
    elif isinstance(data, np.bool_):
        return bool(data)
    elif isinstance(data, np.integer):
        return int(data)
    elif isinstance(data, np.floating):
        return float(data)
    elif isinstance(data, np.ndarray):
        return data.tolist()
    else:
        return data

# Function to calculate Minervini criteria
def calculate_minervini_criteria(ticker_df):
    # Ensure 'close', 'high', and 'low' columns are floats
    ticker_df['close'] = ticker_df['close'].astype(float)
    ticker_df['high'] = ticker_df['high'].astype(float)
    ticker_df['low'] = ticker_df['low'].astype(float)

    # Calculate moving averages
    ticker_df['sma50'] = ticker_df['close'].rolling(window=50).mean()
    ticker_df['sma150'] = ticker_df['close'].rolling(window=150).mean()
    ticker_df['sma200'] = ticker_df['close'].rolling(window=200).mean()
    ticker_df['sma200_22'] = ticker_df['sma200'].shift(22)  # SMA 200 from 22 days ago

    # Get the latest values
    latest_data = ticker_df.iloc[-1]

    # Handle possible NaN values
    if pd.isnull(latest_data[['sma50', 'sma150', 'sma200', 'sma200_22']]).any():
        return None  # Not enough data to calculate moving averages

    # Minervini criteria calculations
    is_price_above_sma_150_and_200 = (latest_data['close'] > latest_data['sma150']) and (latest_data['close'] > latest_data['sma200'])
    is_sma_150_above_sma_200 = latest_data['sma150'] > latest_data['sma200']
    is_sma200_trending_up = latest_data['sma200'] > latest_data['sma200_22']
    is_sma50_above_sma150_and_sma200 = (latest_data['sma50'] > latest_data['sma150']) and (latest_data['sma50'] > latest_data['sma200'])
    is_price_above_sma50 = latest_data['close'] > latest_data['sma50']

    # 52-week high and low
    high_lookback = 260  # Approximately 52 weeks
    low_lookback = 260

    if len(ticker_df) >= high_lookback:
        highest_price = ticker_df['high'].rolling(window=high_lookback).max().iloc[-1]
        lowest_price = ticker_df['low'].rolling(window=low_lookback).min().iloc[-1]
    else:
        highest_price = ticker_df['high'].rolling(window=len(ticker_df)).max().iloc[-1]
        lowest_price = ticker_df['low'].rolling(window=len(ticker_df)).min().iloc[-1]

    is_price_25_percent_above_52_week_low = ((latest_data['close'] / lowest_price) - 1) * 100 >= 25
    is_price_within_25_percent_of_52_week_high = (1 - (latest_data['close'] / highest_price)) * 100 <= 25

    # Compile criteria
    minervini_criteria = {
        'is_price_above_sma_150_and_200': bool(is_price_above_sma_150_and_200),
        'is_sma_150_above_sma_200': bool(is_sma_150_above_sma_200),
        'is_sma200_trending_up': bool(is_sma200_trending_up),
        'is_sma50_above_sma150_and_sma200': bool(is_sma50_above_sma150_and_sma200),
        'is_price_above_sma50': bool(is_price_above_sma50),
        'is_price_25_percent_above_52_week_low': bool(is_price_25_percent_above_52_week_low),
        'is_price_within_25_percent_of_52_week_high': bool(is_price_within_25_percent_of_52_week_high),
        'highest_price_52_week': float(highest_price),
        'lowest_price_52_week': float(lowest_price),
        'sma50': float(latest_data['sma50']),
        'sma150': float(latest_data['sma150']),
        'sma200': float(latest_data['sma200']),
    }

    # Count how many criteria are met
    criteria_flags = [
        minervini_criteria['is_price_above_sma_150_and_200'],
        minervini_criteria['is_sma_150_above_sma_200'],
        minervini_criteria['is_sma200_trending_up'],
        minervini_criteria['is_sma50_above_sma150_and_sma200'],
        minervini_criteria['is_price_above_sma50'],
        minervini_criteria['is_price_25_percent_above_52_week_low'],
        minervini_criteria['is_price_within_25_percent_of_52_week_high'],
    ]
    minervini_criteria['minervini_score'] = int(sum(criteria_flags))
    minervini_criteria['meets_minervini_criteria'] = all(criteria_flags)

    return minervini_criteria

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

                # Calculate Minervini criteria
                minervini_criteria = calculate_minervini_criteria(ticker_df)

                # Check if minervini_criteria is None (not enough data)
                if minervini_criteria is None:
                    print(f"Not enough data to calculate Minervini criteria for {ticker}")
                    continue

                # Store RS score, new RS high status, Minervini criteria, and moving averages
                indicator_data = {
                    "ticker": ticker,
                    "rs_score": float(min(max(rs_score, 1), 99)),  # Ensure RS score is between 1 and 99
                    "new_rs_high": bool(new_rs_high),
                    "date": pd.to_datetime('today'),
                    "minervini_criteria": minervini_criteria,
                }

                # Convert numpy data types to native Python types
                indicator_data = convert_numpy_types(indicator_data)

                indicators_collection.update_one(
                    {"ticker": ticker},
                    {"$set": indicator_data},
                    upsert=True
                )
                
                # Update RS score in OHLCV collection for historical tracking
                ohlcv_collection.update_one(
                    {"ticker": ticker, "date": merged_df['date'].iloc[-1]},
                    {"$set": {"rs_score": rs_score}},
                    upsert=True
                )
                
                print(f"Stored data for {ticker}: RS Score={rs_score}, New RS High={new_rs_high}, Minervini Score={minervini_criteria['minervini_score']}")
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
