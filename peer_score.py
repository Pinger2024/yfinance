import pymongo
import pandas as pd
from datetime import timedelta
from pymongo.errors import AutoReconnect
from pymongo import MongoClient, UpdateOne
import logging
from logging.handlers import RotatingFileHandler
import time
import concurrent.futures
from typing import List, Dict
from functools import wraps

print("Script started")

# Setup logging
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_file = 'peer_score.log'
log_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
log_handler.setFormatter(log_formatter)

logger = logging.getLogger('PeerScoreCalculator')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# Also add a StreamHandler for console output
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

print("Logging configured")

# MongoDB connection setup
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000, serverSelectionTimeoutMS=30000)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

print("MongoDB connection established")

# Configuration
BATCH_SIZE = 100
LOOKBACK_DAYS = 252
MAX_WORKERS = 4

def retry_on_reconnect(max_retries: int = 5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except AutoReconnect as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Max retries reached. Function {func.__name__} failed: {e}")
                        raise
                    logger.warning(f"AutoReconnect error: {e}. Retrying ({attempt + 1}/{max_retries})...")
                    time.sleep(2 ** attempt)  # Exponential backoff
        return wrapper
    return decorator

def normalize_rs_score(rs_raw: float, max_score: float, min_score: float) -> float:
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

@retry_on_reconnect()
def get_ticker_sector(ticker: str) -> str:
    """Fetch the sector for a given ticker from the indicators collection"""
    doc = indicators_collection.find_one({"ticker": ticker}, {"sector": 1})
    if doc and 'sector' in doc:
        return doc['sector']
    return None

@retry_on_reconnect()
def get_peers_in_sector(sector: str, exclude_ticker: str) -> List[str]:
    """Fetch all tickers from the same sector (excluding the given ticker)"""
    peers = indicators_collection.find(
        {"sector": sector, "ticker": {"$ne": exclude_ticker}}, 
        {"ticker": 1}
    )
    return [peer['ticker'] for peer in peers]

@retry_on_reconnect()
def get_stock_data(ticker: str) -> pd.DataFrame:
    """Fetch OHLCV data for a specific ticker from the ohlcv collection"""
    logger.info(f"Fetching data for {ticker}")
    data = list(ohlcv_collection.find({"ticker": ticker}, {"date": 1, "close": 1}).sort("date", 1))
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date')

def process_peer_rs(ticker: str, ticker_df: pd.DataFrame, sector: str, peers: List[str], lookback_days: int) -> List[UpdateOne]:
    """Calculate and store peer RS scores for the ticker against its sector peers"""
    logger.info(f"Processing peer RS for {ticker} in sector: {sector}")
    
    if len(peers) < 1:
        logger.warning(f"Not enough peers for {ticker} in sector: {sector}. Skipping.")
        return []

    # Fetch peer data from OHLCV collection
    peer_data = list(ohlcv_collection.find(
        {"ticker": {"$in": peers}},
        {"date": 1, "close": 1}
    ).sort("date", 1))

    if not peer_data:
        logger.warning(f"No matching data found for {ticker} in sector: {sector}")
        return []

    peer_df = pd.DataFrame(peer_data)
    peer_df['date'] = pd.to_datetime(peer_df['date'])
    
    # Merge with the target ticker data
    merged_df = pd.merge(ticker_df[['close']], peer_df.groupby('date')['close'].mean().rename('peer_close'), on='date')
    merged_df = merged_df.sort_index().reset_index()

    if len(merged_df) < lookback_days:
        logger.warning(f"Not enough data to calculate peer RS for {ticker} in sector: {sector}")
        return []

    # Calculate the RS scores
    periods = [63, 126, 189, 252]
    weights = [2, 1, 1, 1]
    
    updates = []
    for i in range(lookback_days, len(merged_df)):
        rs_values = []
        
        for period, weight in zip(periods, weights):
            if i - period >= 0:
                current_ticker_close = merged_df['close'].iloc[i]
                previous_ticker_close = merged_df['close'].iloc[i - period]
                current_peer_close = merged_df['peer_close'].iloc[i]
                previous_peer_close = merged_df['peer_close'].iloc[i - period]

                rs_value = (current_ticker_close / previous_ticker_close) - (current_peer_close / previous_peer_close)
                rs_values.append(rs_value * weight)
        
        rs_raw = sum(rs_values)
        max_score = sum(weights)
        min_score = -max_score
        peer_rs_score = normalize_rs_score(rs_raw, max_score, min_score)
        peer_rs_score = max(1, min(99, peer_rs_score))
        
        date = merged_df['date'].iloc[i]
        updates.append(UpdateOne(
            {"ticker": ticker, "date": date},
            {"$set": {"peer_rs_sector": peer_rs_score}}
        ))
    
    return updates

@retry_on_reconnect()
def calculate_and_store_sector_peer_rs_scores():
    logger.info("Starting sector peer RS score calculation...")

    # Fetch distinct tickers from the ohlcv collection
    tickers = ohlcv_collection.distinct("ticker")
    
    if not tickers:
        logger.error("No tickers found. Aborting calculation.")
        return

    all_updates = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {}
        
        for ticker in tickers:
            # Fetch sector and peers for each ticker
            sector = get_ticker_sector(ticker)
            if not sector:
                logger.warning(f"No sector found for {ticker}. Skipping.")
                continue
            
            peers = get_peers_in_sector(sector, ticker)
            if not peers:
                logger.warning(f"No peers found in sector {sector} for {ticker}. Skipping.")
                continue
            
            ticker_data = get_stock_data(ticker)
            if not ticker_data.empty:
                future = executor.submit(process_peer_rs, ticker, ticker_data, sector, peers, LOOKBACK_DAYS)
                future_to_ticker[future] = ticker

        # Handle updates in batches
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                updates = future.result()
                all_updates.extend(updates)
                if len(all_updates) >= BATCH_SIZE:
                    ohlcv_collection.bulk_write(all_updates)
                    logger.info(f"Bulk write completed for {len(all_updates)} updates")
                    all_updates = []
            except Exception as exc:
                logger.error(f"{ticker} generated an exception: {exc}")
    
    if all_updates:
        ohlcv_collection.bulk_write(all_updates)
        logger.info(f"Final bulk write completed for {len(all_updates)} updates")

    logger.info("Completed sector peer RS score calculation.")

if __name__ == "__main__":
    try:
        start_time = time.time()
        calculate_and_store_sector_peer_rs_scores()
        end_time = time
