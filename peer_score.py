import pymongo
import pandas as pd
from datetime import timedelta
from pymongo.errors import AutoReconnect
from pymongo import MongoClient
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup with extended timeout
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# Function to normalize RS score to 1-99 range
def normalize_rs_score(rs_raw, max_score, min_score):
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

# Retry decorator for handling AutoReconnect errors
def retry_on_reconnect(func):
    def wrapper(*args, **kwargs):
        retries = 5
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except AutoReconnect as e:
                logging.error(f"AutoReconnect error: {e}. Retrying ({attempt + 1}/{retries})...")
                time.sleep(2 ** attempt)  # Exponential backoff
        raise Exception("Exceeded maximum retry attempts")
    return wrapper

@retry_on_reconnect
def calculate_and_store_peer_rs_scores(batch_size=20, lookback_days=252):
    logging.info("Starting peer RS score calculation (sector and industry)...")
    tickers = indicators_collection.distinct("ticker")
    
    for ticker in tickers:
        logging.info(f"Processing ticker: {ticker}")
        
        # Get the sector and industry for this ticker
        sector_data = indicators_collection.find_one({"ticker": ticker}, {"sector": 1, "industry": 1})
        if not sector_data or not sector_data.get("sector"):
            continue  # Skip if no sector data
        
        sector = sector_data["sector"]
        industry = sector_data.get("industry", None)
        
        tickers_in_sector = indicators_collection.distinct("ticker", {"sector": sector})
        tickers_in_industry = indicators_collection.distinct("ticker", {"industry": industry}) if industry else []
        
        # Fetch stock data for this ticker
        ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))
        if not ticker_data:
            continue
        
        ticker_df = pd.DataFrame(ticker_data)
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])
        
        # Calculate sector peer RS
        if tickers_in_sector:
            process_peer_rs(ticker, ticker_df, "sector", sector, lookback_days)
        
        # Calculate industry peer RS
        if industry and tickers_in_industry:
            process_peer_rs(ticker, ticker_df, "industry", industry, lookback_days)

    logging.info("Completed peer RS score calculation.")

def process_peer_rs(ticker, ticker_df, category, category_value, lookback_days):
    # Fetch peer data from the ohlcv collection
    peer_data = list(ohlcv_collection.find(
        {"ticker": {"$in": indicators_collection.distinct("ticker", {category: category_value}), "$ne": ticker}}
    ).sort("date", 1))

    if not peer_data:
        logging.warning(f"No matching data found for {ticker} in {category}: {category_value}")
        return

    peer_df = pd.DataFrame(peer_data)
    peer_df['date'] = pd.to_datetime(peer_df['date'])
    
    # Merge on 'date' to align the data
    merged_df = pd.merge(ticker_df[['date', 'close']], peer_df[['date', 'close']], on='date', suffixes=('_ticker', '_peer'))
    merged_df = merged_df.sort_values('date').reset_index(drop=True)
    
    if len(merged_df) < lookback_days:
        logging.warning(f"Not enough data to calculate peer RS for {ticker} in {category}: {category_value}")
        return
    
    periods = [63, 126, 189, 252]  # Lookback periods
    weights = [2, 1, 1, 1]  # Weights for different periods
    rs_values = []

    # Calculate the RS score for each period
    for i, period in enumerate(periods):
        if len(merged_df) >= period:
            current_ticker_close = merged_df['close_ticker'].iloc[-1]
            previous_ticker_close = merged_df['close_ticker'].iloc[-period-1]
            current_peer_close = merged_df['close_peer'].iloc[-1]
            previous_peer_close = merged_df['close_peer'].iloc[-period-1]

            rs_value = (current_ticker_close / previous_ticker_close) - (current_peer_close / previous_peer_close)
            rs_values.append(rs_value)
        else:
            rs_values.append(0.0)

    rs_raw = sum([rs_values[i] * weights[i] for i in range(len(rs_values))])
    max_score = sum(weights)
    min_score = -max_score
    peer_rs_score = normalize_rs_score(rs_raw, max_score, min_score)
    peer_rs_score = max(1, min(99, peer_rs_score))  # Ensure the score is between 1 and 99
    
    # Store peer RS score in the ohlcv collection
    ohlcv_collection.update_many(
        {"ticker": ticker, "date": {"$gte": merged_df['date'].min(), "$lte": merged_df['date'].max()}},
        {"$set": {f"peer_rs_{category}": peer_rs_score}}
    )
    
    logging.info(f"Stored peer RS score for {ticker} in {category}: {peer_rs_score}")

if __name__ == "__main__":
    calculate_and_store_peer_rs_scores()
