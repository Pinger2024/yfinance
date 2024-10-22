import logging
from pymongo import MongoClient

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

def ensure_index():
    """Ensure indexes are created on the 'ticker' field for faster queries."""
    logging.info("Ensuring indexes on 'ticker' field for both collections.")
    
    ohlcv_collection.create_index("ticker")
    indicators_collection.create_index("ticker")
    
    logging.info("Indexing complete.")

def remove_l_tickers():
    """Remove tickers ending with .l from both ohlcv_data and indicators collections."""
    try:
        # Ensure indexes on the ticker field to speed up queries
        ensure_index()
        
        # Define the filter for tickers ending with .l
        filter_condition = {"ticker": {"$regex": r"\.l$"}}
        
        logging.info("Starting to remove tickers ending with .l")

        # Remove from ohlcv_data collection
        ohlcv_delete_result = ohlcv_collection.delete_many(filter_condition)
        logging.info(f"Deleted {ohlcv_delete_result.deleted_count} records from ohlcv_data collection")

        # Remove from indicators collection
        indicators_delete_result = indicators_collection.delete_many(filter_condition)
        logging.info(f"Deleted {indicators_delete_result.deleted_count} records from indicators collection")

        logging.info("Completed removal of .l tickers")

    except Exception as e:
        logging.error(f"Error during removal of tickers ending with .l: {str(e)}")

if __name__ == "__main__":
    logging.info("Starting the removal script for .l tickers.")
    remove_l_tickers()
    logging.info("Script completed.")
