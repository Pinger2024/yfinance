import pymongo
import pandas as pd
from pymongo.errors import AutoReconnect
from pymongo import MongoClient, UpdateOne
import logging
from logging.handlers import RotatingFileHandler
import time
import concurrent.futures
from functools import wraps

# Setup logging
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_file = 'peer_score.log'
log_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
log_handler.setFormatter(log_formatter)

logger = logging.getLogger('PeerScoreCalculator')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# MongoDB connection setup
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# Configuration
LOOKBACK_DAYS = 252
MAX_WORKERS = 4

def retry_on_reconnect(max_retries=5):
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
                    time.sleep(2 ** attempt)
        return wrapper
    return decorator

def normalize_rs_score(rs_raw, max_score, min_score):
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

@retry_on_reconnect()
def get_tickers_and_sectors():
    logger.info("Fetching tickers and sectors...")
    tickers_and_sectors = {}
    for doc in indicators_collection.find({}, {'ticker': 1, 'sector': 1}):
        ticker = doc.get('ticker')
        sector = doc.get('sector')
        if ticker and sector:
            tickers_and_sectors[ticker] = sector
        else:
            logger.warning(f"Skipping document due to missing ticker or sector: {doc}")
    return tickers_and_sectors

@retry_on_reconnect()
def get_stock_data(ticker):
    logger.info(f"Fetching data for {ticker}")
    data = list(ohlcv_collection.find({"ticker": ticker}, {"date": 1, "close": 1}).sort("date", 1))
    if data:
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        return df.set_index('date')
    return pd.DataFrame()

def process_peer_rs(ticker, ticker_df, category, category_value, peers):
    if len(peers) < 2:
        logger.warning(f"Not enough peers for {ticker} in {category}: {category_value}. Skipping.")
        return []

    peer_data = list(ohlcv_collection.find(
        {"ticker": {"$in": peers, "$ne": ticker}},
        {"date": 1, "close": 1}
    ).sort("date", 1))

    if not peer_data:
        logger.warning(f"No matching data found for {ticker} in {category}: {category_value}")
        return []

    peer_df = pd.DataFrame(peer_data)
    peer_df['date'] = pd.to_datetime(peer_df['date'])

    merged_df = pd.merge(ticker_df[['close']], peer_df.groupby('date')['close'].mean().rename('peer_close'), on='date')
    merged_df = merged_df.sort_index().reset_index()

    if len(merged_df) < LOOKBACK_DAYS:
        logger.warning(f"Not enough data to calculate peer RS for {ticker} in {category}: {category_value}")
        return []

    periods = [63, 126, 189, 252]
    weights = [2, 1, 1, 1]
    
    updates = []
    for i in range(LOOKBACK_DAYS, len(merged_df)):
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

    if updates:
        ohlcv_collection.bulk_write(updates)
        logger.info(f"Inserted {len(updates)} peer RS scores for {ticker}")

def calculate_and_store_peer_rs_for_ticker(ticker, sector, peers):
    ticker_data = get_stock_data(ticker)
    if not ticker_data.empty:
        process_peer_rs(ticker, ticker_data, "sector", sector, peers)

@retry_on_reconnect()
def calculate_and_store_sector_peer_rs_scores():
    tickers_and_sectors = get_tickers_and_sectors()
    sectors = {}
    for ticker, sector in tickers_and_sectors.items():
        sectors.setdefault(sector, []).append(ticker)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for sector, tickers in sectors.items():
            for ticker in tickers:
                futures.append(executor.submit(calculate_and_store_peer_rs_for_ticker, ticker, sector, tickers))
        concurrent.futures.wait(futures)

if __name__ == "__main__":
    start_time = time.time()
    calculate_and_store_sector_peer_rs_scores()
    logger.info(f"Total execution time: {time.time() - start_time:.2f} seconds")
