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

def calculate_weighted_rs_score(ticker):
    """
    Calculate and rank RS score for a stock using weighted RS values.
    """
    try:
        # Retrieve the most recent trading day
        latest_date = ohlcv_collection.find_one(
            {"ticker": ticker, "$or": [
                {"RS4": {"$exists": True, "$ne": None}},
                {"RS3": {"$exists": True, "$ne": None}},
                {"RS2": {"$exists": True, "$ne": None}},
                {"RS1": {"$exists": True, "$ne": None}},
            ]},
            sort=[("date", -1)],
            projection={"date": 1}
        )

        if not latest_date:
            logging.error(f"No records found with RS values for {ticker}")
            return

        latest_date = latest_date["date"]
        logging.info(f"Latest trading date for RS values: {latest_date}")

        # Fetch RS values for the ticker
        rs_data = ohlcv_collection.find_one(
            {"ticker": ticker, "date": latest_date},
            {"RS1": 1, "RS2": 1, "RS3": 1, "RS4": 1}
        )

        if not rs_data:
            logging.error(f"No RS data found for {ticker} on {latest_date}")
            return

        # Apply the weights: RS1 gets more weight
        weights = {"RS1": 2, "RS2": 1, "RS3": 1, "RS4": 1}
        rs_weighted = (
            (weights["RS1"] * rs_data.get("RS1", 0)) +
            (weights["RS2"] * rs_data.get("RS2", 0)) +
            (weights["RS3"] * rs_data.get("RS3", 0)) +
            (weights["RS4"] * rs_data.get("RS4", 0))
        )

        logging.info(f"{ticker} RS weighted score: {rs_weighted}")
        return {"ticker": ticker, "rs_weighted": rs_weighted}

    except Exception as e:
        logging.error(f"Error calculating RS score for {ticker}: {str(e)}")

def normalize_scores(scores):
    """
    Normalize RS weighted scores into a 1-99 range.
    """
    min_score = min(scores, key=lambda x: x['rs_weighted'])['rs_weighted']
    max_score = max(scores, key=lambda x: x['rs_weighted'])['rs_weighted']

    for score in scores:
        rs_weighted = score['rs_weighted']
        normalized_score = ((rs_weighted - min_score) / (max_score - min_score)) * 98 + 1
        score['rs_score'] = round(normalized_score)

    return scores

def calculate_rank_for_all_stocks():
    """
    Calculate RS scores for all stocks and rank them based on weighted RS scores.
    """
    try:
        # Get all distinct tickers
        tickers = ohlcv_collection.distinct("ticker")
        total_tickers = len(tickers)
        logging.info(f"Total number of tickers: {total_tickers}")

        # Calculate weighted RS scores for all tickers
        scores = []
        for ticker in tickers:
            result = calculate_weighted_rs_score(ticker)
            if result:
                scores.append(result)

        # Normalize the scores to a 1-99 range
        scores_normalized = normalize_scores(scores)

        # Sort stocks by normalized RS score in descending order
        scores_sorted = sorted(scores_normalized, key=lambda x: x["rs_score"], reverse=True)

        # Rank the stocks and update the rank and RS score in the indicators collection
        bulk_operations = []
        for rank, stock in enumerate(scores_sorted, start=1):
            ticker = stock['ticker']
            rs_score = stock['rs_score']

            bulk_operations.append(UpdateOne(
                {"ticker": ticker, "date": latest_date},
                {"$set": {"rs_score": rs_score, "rank": rank}},
                upsert=True
            ))

            logging.info(f"Ticker: {ticker}, Rank: {rank}, RS Score: {rs_score}")

        # Perform the bulk write
        if bulk_operations:
            indicators_collection.bulk_write(bulk_operations, ordered=False)

        logging.info("RS ranking and score calculation completed successfully.")

    except Exception as e:
        logging.error(f"Error during RS ranking and score calculation: {str(e)}")

if __name__ == "__main__":
    calculate_rank_for_all_stocks()
