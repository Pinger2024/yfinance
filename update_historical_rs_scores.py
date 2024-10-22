import pandas as pd
from pymongo import MongoClient, UpdateOne
import logging
from datetime import datetime

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

def calculate_rs_score_for_all_stocks():
    """
    Calculate RS scores for all stocks and store rank and RS score in the 'indicators' collection.
    """
    try:
        # Find the latest date with RS values
        latest_date = ohlcv_collection.find_one(
            {"$or": [
                {"RS4": {"$exists": True, "$ne": None}},
                {"RS3": {"$exists": True, "$ne": None}},
                {"RS2": {"$exists": True, "$ne": None}},
                {"RS1": {"$exists": True, "$ne": None}},
            ]},
            sort=[("date", -1)],
            projection={"date": 1}
        )

        if not latest_date:
            logging.error("No records found with RS values")
            return

        latest_date = latest_date["date"]
        logging.info(f"Latest trading date for RS values: {latest_date}")

        # Get all stocks with RS values for the latest date
        stocks = list(ohlcv_collection.find(
            {"date": latest_date, "$or": [
                {"RS4": {"$exists": True, "$ne": None}},
                {"RS3": {"$exists": True, "$ne": None}},
                {"RS2": {"$exists": True, "$ne": None}},
                {"RS1": {"$exists": True, "$ne": None}},
            ]},
            {"ticker": 1, "RS4": 1, "RS3": 1, "RS2": 1, "RS1": 1}
        ))

        if not stocks:
            logging.error("No RS data found for any stocks on the latest date")
            return

        # Sort stocks by RS value (using fallbacks) in descending order
        stocks.sort(key=lambda x: (
            x.get('RS4') or 
            x.get('RS3') or 
            x.get('RS2') or 
            x.get('RS1') or 
            float('-inf')
        ), reverse=True)

        total_stocks = len(stocks)
        logging.info(f"Processing {total_stocks} stocks for RS score calculation")

        # Prepare bulk operations to store RS scores and ranks in the 'indicators' collection
        bulk_operations = []

        for rank, stock in enumerate(stocks, 1):
            ticker = stock['ticker']
            rs_value = stock.get("RS4") or stock.get("RS3") or stock.get("RS2") or stock.get("RS1")

            # Calculate the RS score as a percentile rank (reverse ranking)
            percentile_rank = ((total_stocks - rank) / total_stocks) * 100
            rs_score = max(1, min(99, round(percentile_rank)))

            logging.info(f"Ticker: {ticker}, Rank: {rank}, RS Score: {rs_score}")

            # Prepare the update document for bulk operation, including rank
            bulk_operations.append(UpdateOne(
                {"ticker": ticker, "date": latest_date},
                {"$set": {"rs_score": rs_score, "rank": rank}},
                upsert=True
            ))

        # Execute bulk update if there are operations
        if bulk_operations:
            indicators_collection.bulk_write(bulk_operations, ordered=False)
            logging.info(f"RS score and rank calculations completed and stored for {total_stocks} stocks")

    except Exception as e:
        logging.error(f"Error calculating RS scores: {str(e)}")

def main():
    logging.info("Starting RS score and rank calculation for all stocks...")
    
    # Run RS score calculation for all stocks
    calculate_rs_score_for_all_stocks()
    
    logging.info("RS score and rank calculation for all stocks complete.")

if __name__ == "__main__":
    main()
