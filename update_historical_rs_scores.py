import pandas as pd
from pymongo import MongoClient
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

def calculate_app_rs_score():
    """
    Calculate and rank RS score for APP stock.
    """
    try:
        # Find the latest date with RS values for APP
        latest_date = ohlcv_collection.find_one(
            {"ticker": "APP", "$or": [
                {"RS4": {"$exists": True, "$ne": None}},
                {"RS3": {"$exists": True, "$ne": None}},
                {"RS2": {"$exists": True, "$ne": None}},
                {"RS1": {"$exists": True, "$ne": None}},
            ]},
            sort=[("date", -1)],
            projection={"date": 1}
        )

        if not latest_date:
            logging.error("No records found with RS values for APP")
            return

        latest_date = latest_date["date"]
        logging.info(f"Latest trading date for RS values: {latest_date}")

        # Get RS values for APP
        app_data = ohlcv_collection.find_one(
            {"ticker": "APP", "date": latest_date},
            {"ticker": 1, "RS4": 1, "RS3": 1, "RS2": 1, "RS1": 1}
        )

        if not app_data:
            logging.error("No RS data found for APP on the latest date")
            return

        rs_value = app_data.get("RS4") or app_data.get("RS3") or app_data.get("RS2") or app_data.get("RS1")

        logging.info(f"APP RS Values - RS1: {app_data.get('RS1')}, RS2: {app_data.get('RS2')}, RS3: {app_data.get('RS3')}, RS4: {app_data.get('RS4')}")
        logging.info(f"APP RS score calculation value (highest available): {rs_value}")

        # Calculate percentile rank for APP based on all stocks for the same date
        stocks = list(ohlcv_collection.find(
            {"date": latest_date, "$or": [
                {"RS4": {"$exists": True, "$ne": None}},
                {"RS3": {"$exists": True, "$ne": None}},
                {"RS2": {"$exists": True, "$ne": None}},
                {"RS1": {"$exists": True, "$ne": None}},
            ]},
            {"ticker": 1, "RS4": 1, "RS3": 1, "RS2": 1, "RS1": 1}
        ))

        # Sort stocks by RS value (using fallbacks) in descending order
        stocks.sort(key=lambda x: (
            x.get('RS4') or 
            x.get('RS3') or 
            x.get('RS2') or 
            x.get('RS1') or 
            float('-inf')
        ), reverse=True)

        # Find rank of APP
        total_stocks = len(stocks)
        app_rank = next((i for i, stock in enumerate(stocks, 1) if stock['ticker'] == "APP"), None)

        if app_rank is None:
            logging.error("APP stock not found in the ranking")
            return

        logging.info(f"APP rank: {app_rank} out of {total_stocks} stocks")

        # Calculate RS score (percentile) -- adjust the logic to reverse the ranking
        percentile_rank = ((total_stocks - app_rank) / total_stocks) * 100
        rs_score = max(1, min(99, round(percentile_rank)))

        logging.info(f"APP RS Score: {rs_score}")

        return rs_score

    except Exception as e:
        logging.error(f"Error calculating RS score for APP: {str(e)}")

if __name__ == "__main__":
    calculate_app_rs_score()
