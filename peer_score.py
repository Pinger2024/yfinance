import pymongo
from pymongo.errors import AutoReconnect
from pymongo import MongoClient
import logging
import pandas as pd
import time

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup with extended timeout
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# Function to retry on AutoReconnect errors
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

# Peer RS calculation for sectors and industries
@retry_on_reconnect
def calculate_and_store_peer_rs_scores(batch_size=20, lookback_days=90):
    logging.info("Starting peer RS score calculation (sector and industry)...")
    
    tickers = indicators_collection.distinct("ticker")
    
    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i+batch_size]
        
        for ticker in batch_tickers:
            logging.info(f"Processing ticker: {ticker}")
            
            # Fetch ticker's data (limit to 'lookback_days' for efficiency)
            ticker_data = list(ohlcv_collection.find(
                {"ticker": ticker}
            ).sort("date", -1).limit(lookback_days))
            
            if not ticker_data:
                continue

            # Create a DataFrame for the ticker data
            ticker_df = pd.DataFrame(ticker_data)
            ticker_df['date'] = pd.to_datetime(ticker_df['date'])

            # Get sector and industry for this ticker
            ticker_info = indicators_collection.find_one({"ticker": ticker}, {"sector": 1, "industry": 1})
            sector = ticker_info.get("sector")
            industry = ticker_info.get("industry")

            # Process Sector Peer RS
            if sector:
                process_peer_rs(ticker, ticker_df, "sector", sector, lookback_days)

            # Process Industry Peer RS
            if industry:
                process_peer_rs(ticker, ticker_df, "industry", industry, lookback_days)
        
    logging.info("Completed peer RS score calculation.")

# Helper function to calculate and store peer RS scores
def process_peer_rs(ticker, ticker_df, category, category_value, lookback_days):
    logging.info(f"Calculating {category} peer RS for {ticker} in {category_value}")

    # Get tickers in the same sector or industry
    peer_tickers = indicators_collection.distinct("ticker", {category: category_value, "ticker": {"$ne": ticker}})

    if not peer_tickers:
        return
    
    # Fetch peer data (limit to 'lookback_days')
    peer_data = list(ohlcv_collection.find(
        {"ticker": {"$in": peer_tickers}}
    ).sort("date", -1).limit(lookback_days))

    if not peer_data:
        return
    
    # Create a DataFrame for peer data and calculate the peer average
    peer_df = pd.DataFrame(peer_data)
    peer_df['date'] = pd.to_datetime(peer_df['date'])
    peer_avg = peer_df.groupby("date")["close"].mean().reset_index()
    peer_avg.rename(columns={"close": f"{category}_avg"}, inplace=True)

    # Merge ticker data with peer average and calculate peer RS
    merged_df = pd.merge(ticker_df, peer_avg, on="date", how="inner")

    # Check if merged_df has rows before proceeding
    if merged_df.empty:
        logging.warning(f"No matching data found for {ticker} in {category}: {category_value}")
        return

    # Calculate peer RS score
    merged_df[f"peer_rs_{category}"] = (merged_df["close"] - merged_df[f"{category}_avg"]) / merged_df[f"{category}_avg"] * 100

    # Update OHLCV collection with peer RS score
    peer_rs_score = merged_df[f"peer_rs_{category}"].iloc[-1]  # Latest peer RS score
    ohlcv_collection.update_many(
        {"ticker": ticker, "date": {"$gte": merged_df['date'].min(), "$lte": merged_df['date'].max()}},
        {"$set": {f"peer_rs_score_{category}": peer_rs_score}}
    )
    logging.info(f"Stored peer RS score for {ticker} in {category}: {peer_rs_score}")

if __name__ == "__main__":
    calculate_and_store_peer_rs_scores(batch_size=20, lookback_days=90)
