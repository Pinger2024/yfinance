import pymongo
import pandas as pd
from datetime import timedelta
from pymongo.errors import AutoReconnect, OperationFailure
from pymongo import MongoClient, UpdateOne
import logging
from logging.handlers import RotatingFileHandler
import time
import concurrent.futures
from typing import List, Dict, Any
import yaml
from functools import wraps
import traceback

# Load configuration
with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Setup logging
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_file = 'peer_score.log'
log_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
log_handler.setFormatter(log_formatter)

logger = logging.getLogger('PeerScoreCalculator')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# MongoDB connection setup
mongo_uri = config['mongo_uri']
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000)
db = client[config['database_name']]
ohlcv_collection = db[config['ohlcv_collection']]
indicators_collection = db[config['indicators_collection']]

def retry_on_error(max_retries: int = 5, delay: int = 1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (AutoReconnect, OperationFailure) as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Max retries reached. Function {func.__name__} failed: {e}")
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds... Error: {str(e)}")
                    logger.warning(traceback.format_exc())  # Log the full stack trace
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
        return wrapper
    return decorator

def normalize_rs_score(rs_raw: float, max_score: float, min_score: float) -> float:
    return max(1, min(99, ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1))

@retry_on_error()
def get_tickers_and_sectors() -> Dict[str, str]:
    logger.info("Fetching tickers and sectors...")
    return {doc['ticker']: doc['sector'] for doc in indicators_collection.find({}, {'ticker': 1, 'sector': 1})}

@retry_on_error()
def get_stock_data(ticker: str) -> pd.DataFrame:
    logger.info(f"Fetching data for {ticker}")
    try:
        data = list(ohlcv_collection.find({"ticker": ticker}, {"date": 1, "close": 1}).sort("date", 1))
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        return df.set_index('date')
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def calculate_rs_score(ticker_data: pd.Series, peer_data: pd.Series, periods: List[int], weights: List[float]) -> float:
    rs_values = []
    try:
        for period, weight in zip(periods, weights):
            if len(ticker_data) >= period and len(peer_data) >= period:
                ticker_return = ticker_data.iloc[-1] / ticker_data.iloc[-period] - 1
                peer_return = peer_data.iloc[-1] / peer_data.iloc[-period] - 1
                rs_values.append((ticker_return - peer_return) * weight)
        rs_raw = sum(rs_values)
        max_score = sum(weights)
        min_score = -max_score
        return normalize_rs_score(rs_raw, max_score, min_score)
    except Exception as e:
        logger.error(f"Error calculating RS score: {str(e)}")
        logger.error(traceback.format_exc())
        return 1  # Fallback to minimum score if something goes wrong

def process_ticker(ticker: str, sector: str, tickers_in_sector: List[str]) -> List[UpdateOne]:
    logger.info(f"Processing {ticker} in sector {sector}")
    ticker_data = get_stock_data(ticker)
    if ticker_data.empty:
        logger.warning(f"No data found for {ticker}")
        return []

    try:
        peer_data = pd.concat([get_stock_data(peer) for peer in tickers_in_sector if peer != ticker], axis=1)
        if peer_data.empty:
            logger.warning(f"No peer data found for {ticker} in sector {sector}")
            return []

        peer_data['mean'] = peer_data.mean(axis=1)
        periods = [63, 126, 189, 252]
        weights = [2, 1, 1, 1]
        updates = []
        for date in ticker_data.index[-config['lookback_days']:]:
            if date in peer_data.index:
                score = calculate_rs_score(
                    ticker_data.loc[:date, 'close'],
                    peer_data.loc[:date, 'mean'],
                    periods,
                    weights
                )
                updates.append(UpdateOne(
                    {"ticker": ticker, "date": date},
                    {"$set": {"peer_rs_sector": score}}
                ))
        return updates
    except Exception as e:
        logger.error(f"Error processing {ticker}: {str(e)}")
        logger.error(traceback.format_exc())
        return []

@retry_on_error()
def calculate_and_store_sector_peer_rs_scores():
    logger.info("Starting sector peer RS score calculation...")
    tickers_and_sectors = get_tickers_and_sectors()
    sectors = {}
    for ticker, sector in tickers_and_sectors.items():
        sectors.setdefault(sector, []).append(ticker)

    all_updates = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=config['max_workers']) as executor:
        future_to_ticker = {executor.submit(process_ticker, ticker, sector, sectors[sector]): ticker 
                            for ticker, sector in tickers_and_sectors.items()}
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                updates = future.result()
                all_updates.extend(updates)
                if len(all_updates) >= config['batch_size']:
                    ohlcv_collection.bulk_write(all_updates)
                    all_updates = []
            except Exception as exc:
                logger.error(f"{ticker} generated an exception: {exc}")
                logger.error(traceback.format_exc())

    if all_updates:
        ohlcv_collection.bulk_write(all_updates)

    logger.info("Completed sector peer RS score calculation.")

if __name__ == "__main__":
    start_time = time.time()
    calculate_and_store_sector_peer_rs_scores()
    end_time = time.time()
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
