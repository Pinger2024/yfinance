import pymongo
import logging
from pymongo import MongoClient
import os
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup
try:
    mongo_uri = os.environ.get('MONGO_URI', 'mongodb://mongodb-9iyq:27017')
    client = MongoClient(mongo_uri)
    db = client['StockData']
    ohlcv_collection = db['ohlcv_data']
    indicators_collection = db['indicators']
    sector_trends_collection = db['sector_trends']
    logging.info("Successfully connected to MongoDB.")
except Exception as e:
    logging.error(f"Error connecting to MongoDB: {e}")
    client = None

# Define the start date (You can change this as needed)
start_date = datetime.strptime("2023-04-15", "%Y-%m-%d")

# Function to calculate sector and industry trends
def calculate_sector_trends():
    if client is None:
        logging.error("MongoDB client is not connected.")
        return

    # Get distinct dates from ohlcv_data for RS scores that are greater than or equal to the start date
    distinct_dates = ohlcv_collection.distinct('date', {'date': {'$gte': start_date}})

    for date in distinct_dates:
        logging.info(f"Processing for date: {date}")

        # Check if sector trends already exist for this date, to avoid redundant processing
        existing_trend = sector_trends_collection.find_one({"date": date})
        if existing_trend:
            logging.info(f"Skipping date {date} as it has already been processed.")
            continue

        # Group tickers by sector from the indicators collection
        sector_pipeline = [
            {"$match": {"sector": {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": "$sector",
                "tickers_in_sector": {"$addToSet": "$ticker"}
            }}
        ]
        sector_data = list(indicators_collection.aggregate(sector_pipeline))

        # Group tickers by industry from the indicators collection
        industry_pipeline = [
            {"$match": {"industry": {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": "$industry",
                "tickers_in_industry": {"$addToSet": "$ticker"}
            }}
        ]
        industry_data = list(indicators_collection.aggregate(industry_pipeline))

        # Process sectors
        for sector in sector_data:
            tickers_in_sector = sector["tickers_in_sector"]
            rs_scores = []
            for ticker in tickers_in_sector:
                # Get the RS score from ohlcv_data for the current date
                rs_score_data = ohlcv_collection.find_one({"ticker": ticker, "date": date}, {"rs_score": 1})
                if rs_score_data and "rs_score" in rs_score_data:
                    rs_scores.append(rs_score_data["rs_score"])

            # Calculate the average RS score for the sector
            if rs_scores:
                average_rs = sum(rs_scores) / len(rs_scores)
                sector_trend = {
                    "date": date,
                    "sector": sector["_id"],
                    "average_rs": average_rs,
                    "tickers_in_sector": tickers_in_sector,
                    "type": "sector"
                }
                sector_trends_collection.update_one(
                    {"date": date, "sector": sector["_id"], "type": "sector"},
                    {"$set": sector_trend},
                    upsert=True
                )
                logging.info(f"Stored sector data for {sector['_id']} on {date}")

        # Process industries
        for industry in industry_data:
            tickers_in_industry = industry["tickers_in_industry"]
            rs_scores = []
            for ticker in tickers_in_industry:
                # Get the RS score from ohlcv_data for the current date
                rs_score_data = ohlcv_collection.find_one({"ticker": ticker, "date": date}, {"rs_score": 1})
                if rs_score_data and "rs_score" in rs_score_data:
                    rs_scores.append(rs_score_data["rs_score"])

            # Calculate the average RS score for the industry
            if rs_scores:
                average_rs = sum(rs_scores) / len(rs_scores)
                industry_trend = {
                    "date": date,
                    "industry": industry["_id"],
                    "average_rs": average_rs,
                    "tickers_in_industry": tickers_in_industry,
                    "type": "industry"
                }
                sector_trends_collection.update_one(
                    {"date": date, "industry": industry["_id"], "type": "industry"},
                    {"$set": industry_trend},
                    upsert=True
                )
                logging.info(f"Stored industry data for {industry['_id']} on {date}")

    logging.info("Completed processing for all dates.")

if __name__ == "__main__":
    logging.info("Starting sector and industry trend calculation script.")
    calculate_sector_trends()
    logging.info("Sector and industry trend calculation completed.")
