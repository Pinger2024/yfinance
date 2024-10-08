import pymongo
import pandas as pd
import numpy as np
from pymongo import MongoClient
import logging
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
indicators_collection = db['indicators']

# Function to normalize RS score between 1-99
def normalize_rs_score(rs_raw, max_score, min_score):
    return ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1

# Function to calculate peer RS score within a group (sector or industry)
def calculate_peer_rs_score(ticker_data, group_data):
    merged_df = pd.merge(ticker_data[['date', 'close']], group_data[['date', 'close', 'ticker']], on='date')
    
    periods = [63, 126, 189, 252]
    weights = [2, 1, 1, 1]
    rs_values = []

    for i, period in enumerate(periods):
        n = min(len(merged_df) - 1, period)
        if n > 0:
            current_ticker_close = merged_df['close_x'].iloc[-1]
            previous_ticker_close = merged_df['close_x'].iloc[-(n+1)]
            
            peer_average_close = merged_df.groupby('date')['close_y'].mean()
            current_peer_close = peer_average_close.iloc[-1]
            previous_peer_close = peer_average_close.iloc[-(n+1)]

            rs_value = (current_ticker_close / previous_ticker_close) - (current_peer_close / previous_peer_close)
            rs_values.append(rs_value)
        else:
            rs_values.append(0.0)

    rs_raw = sum([rs_values[i] * weights[i] for i in range(len(rs_values))])
    max_score = sum(weights)
    min_score = -max_score
    rs_score = normalize_rs_score(rs_raw, max_score, min_score)
    return max(1, min(99, rs_score))

# Function to fetch sector and industry data and calculate peer RS scores
def calculate_and_store_peer_rs_scores():
    sectors = indicators_collection.distinct('sector')
    industries = indicators_collection.distinct('industry')
    
    for ticker in ohlcv_collection.distinct('ticker'):
        ticker_data = list(ohlcv_collection.find({"ticker": ticker}).sort("date", 1))
        if len(ticker_data) == 0:
            logging.warning(f"No data found for ticker: {ticker}")
            continue

        ticker_df = pd.DataFrame(ticker_data)
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])

        # Process sector peer RS score
        sector = indicators_collection.find_one({"ticker": ticker}, {"sector": 1}).get('sector')
        if sector:
            tickers_in_sector = indicators_collection.find({"sector": sector}).distinct('ticker')
            sector_data = list(ohlcv_collection.find({"ticker": {"$in": tickers_in_sector}, "ticker": {"$ne": ticker}}).sort("date", 1))
            
            if len(sector_data) > 0:
                sector_df = pd.DataFrame(sector_data)
                sector_df['date'] = pd.to_datetime(sector_df['date'])
                peer_rs_score_sector = calculate_peer_rs_score(ticker_df, sector_df)
            else:
                peer_rs_score_sector = None
        else:
            peer_rs_score_sector = None
            logging.warning(f"Ticker {ticker} is missing sector data.")

        # Process industry peer RS score (if industry data exists)
        industry = indicators_collection.find_one({"ticker": ticker}, {"industry": 1}).get('industry')
        if industry:
            tickers_in_industry = indicators_collection.find({"industry": industry}).distinct('ticker')
            industry_data = list(ohlcv_collection.find({"ticker": {"$in": tickers_in_industry}, "ticker": {"$ne": ticker}}).sort("date", 1))
            
            if len(industry_data) > 0:
                industry_df = pd.DataFrame(industry_data)
                industry_df['date'] = pd.to_datetime(industry_df['date'])
                peer_rs_score_industry = calculate_peer_rs_score(ticker_df, industry_df)
            else:
                peer_rs_score_industry = None
        else:
            peer_rs_score_industry = None
            logging.warning(f"Ticker {ticker} is missing industry data.")

        # Update the ohlcv_data collection with peer RS scores
        update_data = {}
        if peer_rs_score_sector is not None:
            update_data["peer_rs_score_sector"] = peer_rs_score_sector
        if peer_rs_score_industry is not None:
            update_data["peer_rs_score_industry"] = peer_rs_score_industry

        if update_data:
            ohlcv_collection.update_many(
                {"ticker": ticker, "date": {"$gte": ticker_df['date'].min(), "$lte": ticker_df['date'].max()}},
                {"$set": update_data}
            )
            logging.info(f"Stored peer RS scores for {ticker}: {update_data}")

if __name__ == "__main__":
    logging.info("Starting peer RS score calculation (sector and industry)...")
    calculate_and_store_peer_rs_scores()
    logging.info("Peer RS score calculation (sector and industry) completed.")
