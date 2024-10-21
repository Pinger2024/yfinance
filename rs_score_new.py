from pymongo import MongoClient
import pandas as pd
import logging
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

def calculate_rs_ranking():
    # Fetch all stocks with RS4 values
    cursor = ohlcv_collection.find(
        {"RS4": {"$exists": True}},
        {"ticker": 1, "RS4": 1}
    ).batch_size(1000)  # Adjust batch size as needed

    # Process data in chunks
    chunk_size = 10000
    all_rs4_values = []
    
    for chunk in pd.read_csv(cursor, chunksize=chunk_size):
        all_rs4_values.extend(chunk['RS4'].tolist())
    
    # Calculate percentile ranks for all RS4 values
    percentile_ranks = pd.Series(all_rs4_values).rank(pct=True) * 100
    
    # Reset cursor to start
    cursor.rewind()
    
    bulk_ops = []
    for i, doc in enumerate(cursor):
        rs_score = max(1, min(99, round(percentile_ranks[i])))
        bulk_ops.append(
            UpdateOne(
                {"ticker": doc['ticker']},
                {"$set": {"rs_score": rs_score}},
                upsert=True
            )
        )
        
        if len(bulk_ops) == chunk_size:
            try:
                indicators_collection.bulk_write(bulk_ops, ordered=False)
            except BulkWriteError as bwe:
                logging.warning(f"Bulk write error: {bwe.details}")
            bulk_ops = []

    # Write any remaining operations
    if bulk_ops:
        try:
            indicators_collection.bulk_write(bulk_ops, ordered=False)
        except BulkWriteError as bwe:
            logging.warning(f"Bulk write error: {bwe.details}")

    logging.info("RS4 ranking and score calculation completed.")

    # Log TSLA info if available
    tsla_info = indicators_collection.find_one({"ticker": "TSLA"})
    if tsla_info and 'rs_score' in tsla_info:
        logging.info(f"TSLA RS Score: {tsla_info['rs_score']}")
    else:
        logging.warning("TSLA RS Score not found.")

if __name__ == "__main__":
    calculate_rs_ranking()