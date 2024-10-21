from pymongo import MongoClient
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# Function to normalize RS score to 1-99 range
def normalize_rs_score(rs_raw, max_score, min_score):
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

# Function to rank stocks based on RS4
def calculate_rs_ranking():
    # Fetch RS4 values for all stocks
    all_stocks_data = list(ohlcv_collection.aggregate([
        {"$match": {"RS4": {"$exists": True}}},
        {"$project": {"ticker": 1, "RS4": 1}}
    ]))

    # Convert the data to a DataFrame for easier ranking
    df = pd.DataFrame(all_stocks_data)
    
    # Check if there's data to process
    if df.empty:
        logging.error("No RS4 data available for any stocks.")
        return

    # Rank stocks by RS4 value (12-month cumulative return)
    df['rank'] = df['RS4'].rank(pct=True) * 100  # Percentile rank
    
    # Ensure RS score is between 1 and 99
    df['rs_score'] = df['rank'].apply(lambda x: max(1, min(99, round(x))))

    # Log the ranking result for TSLA as an example
    tsla_rank = df[df['ticker'] == 'TSLA']
    if not tsla_rank.empty:
        logging.info(f"TSLA RS4: {tsla_rank.iloc[0]['RS4']}, RS Score: {tsla_rank.iloc[0]['rs_score']}")
    else:
        logging.warning("TSLA not found in the RS4 data.")

    # Store the RS score in the indicators collection
    for index, row in df.iterrows():
        indicators_collection.update_one(
            {"ticker": row['ticker']},
            {"$set": {"rs_score": row['rs_score']}},
            upsert=True
        )
    
    logging.info("RS4 ranking and score calculation completed.")

if __name__ == "__main__":
    calculate_rs_ranking()
