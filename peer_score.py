import pymongo
from pymongo.errors import AutoReconnect
from pymongo import MongoClient
import logging
import pandas as pd
import time

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup with extended timeout
mongo_uri = 'mongodb://mongodb-9iyq:27017'
client = MongoClient(mongo_uri, connectTimeoutMS=60000, socketTimeoutMS=60000)
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# Function to retry on AutoReconnect errors
def retry_on_reconnect(func):
    def wrapper(*args, **kwargs):
        retries = 5
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except AutoReconnect as e:
                logging.error(f"AutoReconnect error: {e}. Retrying ({attempt + 1}/{retries})...")
                time.sleep(2 ** attempt)  # Exponential backoff
        raise Exception("Exceeded maximum retry attempts")
    return wrapper

# Normalize function (in case you want to normalize peer RS scores)
def normalize_peer_rs_score(rs_raw, max_score, min_score):
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

# Calculate peer RS scores and store in MongoDB
@retry_on_reconnect
def calculate_and_store_peer_rs_scores():
    logging.info("Starting peer RS score calculation (sector and industry)...")
    tickers = indicators_collection.distinct("ticker")
    
    for ticker in tickers:
        logging.info(f"Processing ticker: {ticker}")
        ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))
        if not ticker_data:
            continue

        ticker_df = pd.DataFrame(ticker_data)
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])

        # Get the sector and industry of the ticker from the indicators collection
        ticker_info = indicators_collection.find_one({"ticker": ticker}, {"sector": 1, "industry": 1})
        sector = ticker_info.get("sector")
        industry = ticker_info.get("industry")

        # Sector RS calculation
        if sector:
            tickers_in_sector = list(indicators_collection.find({"sector": sector}).distinct("ticker"))
            if tickers_in_sector:
                sector_data = list(ohlcv_collection.find(
                    {"ticker": {"$in": tickers_in_sector}, "ticker": {"$ne": ticker}}).sort("date", 1))

                if sector_data:
                    sector_df = pd.DataFrame(sector_data)
                    sector_df['date'] = pd.to_datetime(sector_df['date'])
                    sector_avg = sector_df.groupby("date")["close"].mean().reset_index()
                    sector_avg.rename(columns={"close": "sector_avg"}, inplace=True)

                    merged_df = pd.merge(ticker_df, sector_avg, on="date", how="inner")
                    merged_df["peer_rs"] = (merged_df["close"] - merged_df["sector_avg"]) / merged_df["sector_avg"] * 100
                    peer_rs_score = merged_df["peer_rs"].iloc[-1]

                    # Update the OHLCV collection with the new peer RS score
                    ohlcv_collection.update_many(
                        {"ticker": ticker, "date": {"$gte": merged_df['date'].min(), "$lte": merged_df['date'].max()}},
                        {"$set": {"peer_rs_score_sector": peer_rs_score}}
                    )
                    logging.info(f"Stored peer RS score for {ticker} in sector: {peer_rs_score}")

        # Industry RS calculation
        if industry:
            tickers_in_industry = list(indicators_collection.find({"industry": industry}).distinct("ticker"))
            if tickers_in_industry:
                industry_data = list(ohlcv_collection.find(
                    {"ticker": {"$in": tickers_in_industry}, "ticker": {"$ne": ticker}}).sort("date", 1))

                if industry_data:
                    industry_df = pd.DataFrame(industry_data)
                    industry_df['date'] = pd.to_datetime(industry_df['date'])
                    industry_avg = industry_df.groupby("date")["close"].mean().reset_index()
                    industry_avg.rename(columns={"close": "industry_avg"}, inplace=True)

                    merged_df_industry = pd.merge(ticker_df, industry_avg, on="date", how="inner")
                    merged_df_industry["peer_rs_industry"] = (merged_df_industry["close"] - merged_df_industry["industry_avg"]) / merged_df_industry["industry_avg"] * 100
                    peer_rs_score_industry = merged_df_industry["peer_rs_industry"].iloc[-1]

                    # Update the OHLCV collection with the new peer RS score for industry
                    ohlcv_collection.update_many(
                        {"ticker": ticker, "date": {"$gte": merged_df_industry['date'].min(), "$lte": merged_df_industry['date'].max()}},
                        {"$set": {"peer_rs_score_industry": peer_rs_score_industry}}
                    )
                    logging.info(f"Stored peer RS score for {ticker} in industry: {peer_rs_score_industry}")

    logging.info("Completed peer RS score calculation.")

if __name__ == "__main__":
    calculate_and_store_peer_rs_scores()
