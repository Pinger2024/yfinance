import pymongo
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
def calculate_and_store_peer_rs_scores():
    logging.info("Starting peer RS score calculation (sector and industry)...")
    tickers = indicators_collection.distinct("ticker")
    
    for ticker in tickers:
        # Get the sector for this ticker
        sector_data = indicators_collection.find_one({"ticker": ticker}, {"sector": 1, "industry": 1})
        if not sector_data or not sector_data.get("sector"):
            continue  # Skip if no sector data
        
        sector = sector_data["sector"]
        industry = sector_data.get("industry", None)
        
        tickers_in_sector = indicators_collection.distinct("ticker", {"sector": sector})
        tickers_in_industry = indicators_collection.distinct("ticker", {"industry": industry}) if industry else []

        # Fetch RS scores for peers
        sector_data = list(ohlcv_collection.find(
            {"ticker": {"$in": tickers_in_sector}, "ticker": {"$ne": ticker}}
        ).sort("date", 1))

        # Calculate peer RS scores (sector level)
        if sector_data:
            # Calculate and store peer RS score logic here...

        if industry and tickers_in_industry:
            industry_data = list(ohlcv_collection.find(
                {"ticker": {"$in": tickers_in_industry}, "ticker": {"$ne": ticker}}
            ).sort("date", 1))

            # Calculate and store peer RS score (industry level) here...

    logging.info("Completed peer RS score calculation.")

if __name__ == "__main__":
    calculate_and_store_peer_rs_scores()
