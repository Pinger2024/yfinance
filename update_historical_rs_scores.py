import pandas as pd
from pymongo import MongoClient
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Function to rank stocks based on weighted RS values
def rank_and_assign_rs_scores():
    try:
        # Find the latest date with RS values from the OHLCV collection
        latest_date = ohlcv_collection.find_one(
            {"RS4": {"$exists": True}},
            sort=[("date", -1)],
            projection={"date": 1}
        )

        if not latest_date:
            logging.error("No records found with RS values in OHLCV")
            return

        latest_date = latest_date["date"]
        logging.info(f"Latest trading date for RS values: {latest_date}")

        # Fetch all stocks with RS1, RS2, RS3, RS4 values for the latest date
        stocks = list(ohlcv_collection.find(
            {"date": latest_date, "RS4": {"$exists": True}},
            {"ticker": 1, "RS1": 1, "RS2": 1, "RS3": 1, "RS4": 1}
        ))

        if not stocks:
            logging.error(f"No RS values found for date: {latest_date}")
            return

        # Apply weighting to RS values (more weight to recent performance)
        w1, w2, w3, w4 = 2, 1, 1, 1  # Weighting factors
        for stock in stocks:
            rs1, rs2, rs3, rs4 = stock.get('RS1'), stock.get('RS2'), stock.get('RS3'), stock.get('RS4')

            # Calculate the weighted RS score
            rs_raw = (w1 * rs1 + w2 * rs2 + w3 * rs3 + w4 * rs4) / (w1 + w2 + w3 + w4)
            stock['rs_raw'] = rs_raw

        # Rank stocks based on the weighted RS score
        stocks.sort(key=lambda x: x['rs_raw'], reverse=True)

        # Assign RS score based on rank
        total_stocks = len(stocks)
        bulk_operations = []
        for rank, stock in enumerate(stocks, 1):
            ticker = stock['ticker']
            rs_score = max(1, min(99, round((rank / total_stocks) * 100)))

            # Store RS score and rank in the indicators collection
            bulk_operations.append(
                {"update_one": {
                    "filter": {"ticker": ticker, "date": latest_date},
                    "update": {"$set": {"rs_score": rs_score, "rank": rank}},
                    "upsert": True
                }}
            )

            logging.info(f"Ticker: {ticker}, Rank: {rank}, RS Score: {rs_score}")

        # Perform bulk write to store RS scores and ranks
        if bulk_operations:
            indicators_collection.bulk_write(bulk_operations)

        logging.info("RS score calculation and ranking completed.")
    
    except Exception as e:
        logging.error(f"Error in ranking and assigning RS scores: {str(e)}")

# Main function to run the process
def main():
    # Rank and assign RS scores for all stocks based on the latest RS values
    rank_and_assign_rs_scores()

if __name__ == "__main__":
    main()
