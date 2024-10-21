from pymongo import MongoClient
import pandas as pd
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
    # Fetch all stocks with RS4 values at once
    cursor = ohlcv_collection.find(
        {"RS4": {"$exists": True, "$ne": None}},  # Exclude null RS4 values
        {"ticker": 1, "RS4": 1}
    )
    
    # Convert the cursor to a DataFrame for easier manipulation
    all_data = [(doc['ticker'], doc['RS4']) for doc in cursor if isinstance(doc['RS4'], (int, float))]
    df = pd.DataFrame(all_data, columns=['ticker', 'RS4'])

    # Rank all stocks at once
    df['rank'] = df['RS4'].rank(pct=True, method='min') * 100  # Rank and normalize to 1-99
    df['rs_score'] = df['rank'].apply(lambda x: max(1, min(99, round(float(x)))) if pd.notna(x) else 1)

    # Prepare bulk update operations
    bulk_ops = [
        UpdateOne(
            {"ticker": row['ticker']},
            {"$set": {"rs_score": int(row['rs_score'])}},
            upsert=True
        )
        for _, row in df.iterrows()
    ]
    
    # Execute all updates at once
    try:
        result = indicators_collection.bulk_write(bulk_ops, ordered=False)
        logging.info(f"RS scores updated: {result.modified_count} modified")
    except BulkWriteError as bwe:
        logging.warning(f"Bulk write error: {bwe.details}")

if __name__ == "__main__":
    calculate_rs_ranking()
