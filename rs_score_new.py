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
    # Step 1: Find the latest trading date where RS4 exists
    latest_date = ohlcv_collection.find_one(
        {"RS4": {"$exists": True, "$ne": None}},
        sort=[("date", -1)],  # Sort by date descending
        projection={"date": 1}
    )["date"]
    
    logging.info(f"Latest trading date for RS4 values: {latest_date}")
    
    # Step 2: Fetch all RS4 values for the latest date, sorted by RS4
    cursor = ohlcv_collection.find(
        {"RS4": {"$exists": True, "$ne": None}, "date": latest_date},
        {"ticker": 1, "RS4": 1}
    ).sort("RS4", 1)
    
    total_stocks = ohlcv_collection.count_documents({"RS4": {"$exists": True, "$ne": None}, "date": latest_date})
    rank = 1
    bulk_ops = []
    
    logging.info(f"Total stocks with RS4 for the latest date: {total_stocks}")
    
    # Step 3: Iterate through the cursor and rank each stock
    for doc in cursor:
        ticker = doc['ticker']
        rs4_value = doc['RS4']
        
        # Calculate percentile rank (convert rank to 1-99 scale)
        percentile_rank = (rank / total_stocks) * 100
        rs_score = max(1, min(99, round(percentile_rank)))  # Ensure it's between 1 and 99
        
        # Log ticker rank info
        logging.info(f"Ticker: {ticker}, RS4: {rs4_value}, Rank: {rank}, RS Score: {rs_score}")
        
        # Add to bulk update operations
        bulk_ops.append(
            UpdateOne({"ticker": ticker, "date": latest_date}, {"$set": {"rs_score": rs_score}}, upsert=True)
        )
        
        # Increment rank
        rank += 1
        
        # Step 4: Execute bulk write every 1000 records
        if len(bulk_ops) >= 1000:
            try:
                result = indicators_collection.bulk_write(bulk_ops, ordered=False)
                logging.info(f"Processed chunk: {result.modified_count} modified")
            except BulkWriteError as bwe:
                logging.warning(f"Bulk write error: {bwe.details}")
            bulk_ops = []  # Reset for next chunk
    
    # Step 5: Execute any remaining bulk operations
    if bulk_ops:
        try:
            result = indicators_collection.bulk_write(bulk_ops, ordered=False)
            logging.info(f"Processed chunk: {result.modified_count} modified")
        except BulkWriteError as bwe:
            logging.warning(f"Bulk write error: {bwe.details}")

    logging.info("RS4 ranking and score calculation completed.")

if __name__ == "__main__":
    calculate_rs_ranking()
