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
    sector_trends_collection = db['sector_trends']
    logging.info("Successfully connected to MongoDB.")
except Exception as e:
    logging.error(f"Error connecting to MongoDB: {e}")
    client = None

# Function to calculate sector and industry trends
def calculate_sector_trends():
    if client is None:
        logging.error("MongoDB client is not connected.")
        return
    
    # Get distinct dates
    distinct_dates = ohlcv_collection.distinct('date')
    
    for date in distinct_dates:
        logging.info(f"Processing for date: {date}")

        # Aggregate sector data
        pipeline = [
            {"$match": {"date": date, "sector": {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": "$sector",
                "average_rs": {"$avg": "$rs_score"},
                "tickers_in_sector": {"$addToSet": "$ticker"}
            }}
        ]
        sector_data = list(ohlcv_collection.aggregate(pipeline))

        # Debugging: Log the sector data
        logging.info(f"Sector data for {date}: {sector_data}")

        # Aggregate industry data
        industry_pipeline = [
            {"$match": {"date": date, "industry": {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": "$industry",
                "average_rs": {"$avg": "$rs_score"},
                "tickers_in_industry": {"$addToSet": "$ticker"}
            }}
        ]
        industry_data = list(ohlcv_collection.aggregate(industry_pipeline))

        # Debugging: Log the industry data
        logging.info(f"Industry data for {date}: {industry_data}")

        # Store the results in sector_trends collection
        for sector in sector_data:
            sector_trend = {
                "date": date,
                "sector": sector["_id"],
                "average_rs": sector["average_rs"],
                "tickers_in_sector": sector["tickers_in_sector"],
                "type": "sector"
            }
            try:
                logging.info(f"Attempting to insert/update sector data for {sector['_id']} on {date}")
                sector_trends_collection.update_one(
                    {"date": date, "sector": sector["_id"], "type": "sector"},
                    {"$set": sector_trend},
                    upsert=True
                )
                logging.info(f"Stored sector data for {sector['_id']} on {date}")
            except Exception as e:
                logging.error(f"Error inserting sector data for {sector['_id']} on {date}: {e}")

        for industry in industry_data:
            industry_trend = {
                "date": date,
                "industry": industry["_id"],
                "average_rs": industry["average_rs"],
                "tickers_in_industry": industry["tickers_in_industry"],
                "type": "industry"
            }
            try:
                logging.info(f"Attempting to insert/update industry data for {industry['_id']} on {date}")
                sector_trends_collection.update_one(
                    {"date": date, "industry": industry["_id"], "type": "industry"},
                    {"$set": industry_trend},
                    upsert=True
                )
                logging.info(f"Stored industry data for {industry['_id']} on {date}")
            except Exception as e:
                logging.error(f"Error inserting industry data for {industry['_id']} on {date}: {e}")

        logging.info(f"Finished processing for {date}.")

    logging.info("Completed processing for all dates.")

if __name__ == "__main__":
    logging.info("Starting sector and industry trend calculation script.")
    calculate_sector_trends()
    logging.info("Sector and industry trend calculation completed.")
