from pymongo import MongoClient
import pandas as pd
import numpy as np
import logging
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

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
    all_data = []
    
    for doc in cursor:
        all_data.append((doc['ticker'], doc['RS4']))
        
        if len(all_data) >= chunk_size:
            process_chunk(all_data)
            all_data = []
    
    # Process any remaining data
    if all_data:
        process_chunk(all_data)

    logging.info("RS4 ranking and score calculation completed.")

    # Log TSLA info if available
    tsla_info = indicators_collection.find_one({"ticker": "TSLA"})
    if tsla_info and 'rs_score' in tsla_info:
        logging.info(f"TSLA RS Score: {tsla_info['rs_score']}")
    else:
        logging.warning("TSLA RS Score not found.")

def process_chunk(data):
    df = pd.DataFrame(data, columns=['ticker', 'RS4'])
    df['rank'] = df['RS4'].rank(pct=True) * 100
    df['rs_score'] = df['rank'].apply(lambda x: max(1, min(99, round(x))))

    bulk_ops = [
        UpdateOne(
            {"ticker": row['ticker']},
            {"$set": {"rs_score": row['rs_score']}},
            upsert=True
        )
        for _, row in df.iterrows()
    ]

    try:
        indicators_collection.bulk_write(bulk_ops, ordered=False)
    except BulkWriteError as bwe:
        logging.warning(f"Bulk write error: {bwe.details}")

if __name__ == "__main__":
    calculate_rs_ranking()