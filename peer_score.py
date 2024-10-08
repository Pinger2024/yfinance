import pymongo
import pandas as pd
from datetime import timedelta
from pymongo.errors import AutoReconnect
from pymongo import MongoClient, UpdateOne
import logging
from logging.handlers import RotatingFileHandler
import time
import concurrent.futures
from typing import List, Dict, Any
from functools import wraps

# Setup logging
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_file = 'peer_score.log'
log_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
log_handler.setFormatter(log_formatter)

logger = logging.getLogger('PeerScoreCalculator')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# MongoDB connection setup
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000, serverSelectionTimeoutMS=30000)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

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
def get_tickers_and_sectors() -> Dict[str, str]:
    logger.info("Fetching tickers and sectors...")
    tickers_and_sectors = {}
    for doc in indicators_collection.find({}, {'ticker': 1, 'sector': 1}):
        ticker = doc.get('ticker')
        sector = doc.get('sector')
        if ticker and sector:
            tickers_and_sectors[ticker] = sector
        else:
            logger.warning(f"Skipping document due to missing ticker or sector: {doc}")
    logger.info(f"Found {len(tickers_and_sectors)} valid ticker-sector pairs")
    return tickers_and_sectors

@retry_on_reconnect()
def get_stock_data(ticker: str) -> pd.DataFrame:
    logger.info(f"Fetching data for {ticker}")
    data = list(ohlcv_collection.find({"ticker": ticker}, {"date": 1, "close": 1}).sort("date", 1))
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date')

def process_peer_rs(ticker: str, ticker_df: pd.DataFrame, category: str, category_value: str, tickers_in_category: List[str], lookback_days: int) -> List[UpdateOne]:
    logger.info(f"Processing peer RS for {ticker} in {category}: {category_value}")
    
    if len(tickers_in_category) < 2:  # Need at least one peer
        logger.warning(f"Not enough peers for {ticker} in {category}: {category_value}. Skipping.")
        return []
    
    peer_data = list(ohlcv_collection.find(
        {"ticker": {"$in": tickers_in_category, "$ne": ticker}},
        {"date": 1, "close": 1}
    ).sort("date", 1))

    if not peer_data:
        logger.warning(f"No matching data found for {ticker} in {category}: {category_value}")
        return []

    peer_df = pd.DataFrame(peer_data)
    peer_df['date'] = pd.to_datetime(peer_df['date'])
    
    merged_df = pd.merge(ticker_df[['close']], peer_df.groupby('date')['close'].mean().rename('peer_close'), on='date')
    merged_df = merged_df.sort_index().reset_index()
    
    if len(merged_df) < lookback_days:
        logger.warning(f"Not enough data to calculate peer RS for {ticker} in {category}: {category_value}")
        return []

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
            {"$set": {f"peer_rs_{category}": peer_rs_score}}
        ))
    
    return updates

@retry_on_reconnect()
def calculate_and_store_sector_peer_rs_scores():
    logger.info("Starting sector peer RS score calculation...")
    
    tickers_and_sectors = get_tickers_and_sectors()
    if not tickers_and_sectors:
        logger.error("No valid ticker-sector pairs found. Aborting calculation.")
        return

    sectors = {}
    for ticker, sector in tickers_and_sectors.items():
        sectors.setdefault(sector, []).append(ticker)
    
    logger.info(f"Processing {len(tickers_and_sectors)} tickers across {len(sectors)} sectors")
    
    all_updates = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {}
        for ticker, sector in tickers_and_sectors.items():
            ticker_data = get_stock_data(ticker)
            if not ticker_data.empty:
                future = executor.submit(process_peer_rs, ticker, ticker_data, "sector", sector, sectors[sector], LOOKBACK_DAYS)
                future_to_ticker[future] = ticker

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
        end_time = time.time()
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")