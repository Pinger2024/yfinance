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
indicators_collection = db['indicators']

# Function to calculate RS score using weighted method
def calculate_rs_scores(ticker):
    """
    Calculate and store the RS scores for the given ticker based on weighted recent performance.
    """
    try:
        # Fetch the last 252 trading days' data for the ticker
        history = list(ohlcv_collection.find(
            {"ticker": ticker},
            {"date": 1, "close": 1, "_id": 0}
        ).sort("date", -1).limit(252))

        if len(history) < 252:
            logging.warning(f"Not enough data to calculate RS for {ticker}")
            return

        df = pd.DataFrame(history).sort_values('date')

        # Lengths for RS periods (e.g., ~3 months, ~6 months, etc.)
        n63 = min(63, len(df))
        n126 = min(126, len(df))
        n189 = min(189, len(df))
        n252 = min(252, len(df))

        # Calculate RS1, RS2, RS3, RS4 using the weighted formula
        benchmark_close = ohlcv_collection.find_one(
            {"ticker": "SPY", "date": df['date'].iloc[-1]},
            {"close": 1}
        )['close']  # Assuming "SPY" is the benchmark like the S&P 500

        rs1 = ((df['close'].iloc[-1] / df['close'].iloc[-n63]) - (benchmark_close / df['close'].iloc[-n63])) * 100
        rs2 = ((df['close'].iloc[-1] / df['close'].iloc[-n126]) - (benchmark_close / df['close'].iloc[-n126])) * 100
        rs3 = ((df['close'].iloc[-1] / df['close'].iloc[-n189]) - (benchmark_close / df['close'].iloc[-n189])) * 100
        rs4 = ((df['close'].iloc[-1] / df['close'].iloc[-n252]) - (benchmark_close / df['close'].iloc[-n252])) * 100

        # Weights (giving more weight to RS1)
        w1, w2, w3, w4 = 2, 1, 1, 1

        # Raw RS score (weighted sum)
        rs_raw = w1 * rs1 + w2 * rs2 + w3 * rs3 + w4 * rs4

        # Max and Min scores for normalization
        max_score = w1 + w2 + w3 + w4
        min_score = -max_score

        # Normalize RS score to the range of 1-99
        rs_score = ((rs_raw - min_score) / (max_score - min_score)) * 98 + 1
        rs_score = max(1, min(99, rs_score))

        logging.info(f"{ticker} RS Score: {rs_score}")

        # Store the RS score in the indicators collection
        indicators_collection.update_one(
            {"ticker": ticker, "date": df['date'].iloc[-1]},
            {"$set": {"rs_score": rs_score}},
            upsert=True
        )

    except Exception as e:
        logging.error(f"Error calculating RS score for {ticker}: {str(e)}")

# Run the RS calculation for all tickers
def main():
    tickers = ohlcv_collection.distinct("ticker")
    total_tickers = len(tickers)

    logging.info(f"Starting RS score calculation for {total_tickers} tickers")

    for ticker in tickers:
        calculate_rs_scores(ticker)

    logging.info("RS score calculation completed")

if __name__ == "__main__":
    main()
