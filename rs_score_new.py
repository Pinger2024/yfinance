from pymongo import MongoClient, UpdateOne
import logging
from pymongo.errors import BulkWriteError

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

def calculate_rs_ranking():
    # Step 1: Fetch all RS4 values, sorted by RS4 in ascending order
    cursor = ohlcv_collection.find(
        {"RS4": {"$exists": True, "$ne": None}},  # Exclude null RS4 values
        {"ticker": 1, "RS4": 1}
    ).sort("RS4", 1)  # Sort by RS4 in ascending order

    total_stocks = ohlcv_collection.count_documents({"RS4": {"$exists": True, "$ne": None}})
    rank = 1
    bulk_ops = []
    
    logging.info(f"Total stocks with RS4: {total_stocks}")
    
    # Step 2: Iterate through the cursor and rank each stock
    for doc in cursor:
        ticker = doc['ticker']
        rs4_value = doc['RS4']
        
        # Calculate percentile rank (convert rank to 1-99 scale)
        percentile_rank = (rank / total_stocks) * 100
        rs_score = max(1, min(99, round(percentile_rank)))  # Ensure it's between 1 and 99
        
        # Add to bulk update operations
        bulk_ops.append(
            UpdateOne({"ticker": ticker}, {"$set": {"rs_score": rs_score}}, upsert=True)
        )
        
        # Log individual stock rank info
        logging.info(f"Ticker: {ticker}, RS4: {rs4_value}, Rank: {rank}, RS Score: {rs_score}")
        
        # Increment rank
        rank += 1
        
        # Step 3: Execute bulk write every 1000 records
        if len(bulk_ops) >= 1000:
            try:
                result = indicators_collection.bulk_write(bulk_ops, ordered=False)
                logging.info(f"Processed chunk: {result.modified_count} modified")
            except BulkWriteError as bwe:
                logging.warning(f"Bulk write error: {bwe.details}")
            bulk_ops = []  # Reset for next chunk
    
    # Step 4: Execute any remaining bulk operations
    if bulk_ops:
        try:
            result = indicators_collection.bulk_write(bulk_ops, ordered=False)
            logging.info(f"Processed chunk: {result.modified_count} modified")
        except BulkWriteError as bwe:
            logging.warning(f"Bulk write error: {bwe.details}")

    logging.info("RS4 ranking and score calculation completed.")

if __name__ == "__main__":
    calculate_rs_ranking()
