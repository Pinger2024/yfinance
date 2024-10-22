import pandas as pd
from pymongo import MongoClient, UpdateOne
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

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

def calculate_rs_scores(ticker):
    """
    Calculate RS scores for all dates for a given ticker.
    """
    try:
        # Fetch the latest 252 days of historical data for the ticker
        history = list(ohlcv_collection.find(
            {"ticker": ticker},
            {"date": 1, "close": 1, "_id": 0}
        ))

        if not history:
            return f"No data found for {ticker}"

        df = pd.DataFrame(history)
        df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last').reset_index(drop=True)

        # Compute daily_pct_change
        df['daily_pct_change'] = df['close'].pct_change() * 100

        # Compute RS values using shifted close prices
        periods = {
            "RS1": 63,
            "RS2": 126,
            "RS3": 189,
            "RS4": 252
        }

        for rs_key, period in periods.items():
            df[f'close_shift_{period}'] = df['close'].shift(period)
            df[rs_key] = (df['close'] - df[f'close_shift_{period}']) / df[f'close_shift_{period}'] * 100

        # Drop rows where 'daily_pct_change' is NaN (first row)
        df = df.dropna(subset=['daily_pct_change']).reset_index(drop=True)

        # Prepare bulk operations for updating the RS and daily_pct_change values
        bulk_operations = []
        batch_size = 100  # Control batch size to avoid memory issues

        for idx, row in df.iterrows():
            current_date = row['date']
            update_doc = {
                "daily_pct_change": row['daily_pct_change']
            }

            for rs_key in periods.keys():
                if pd.notnull(row[rs_key]):
                    update_doc[rs_key] = row[rs_key]

            bulk_operations.append(UpdateOne(
                {"ticker": ticker, "date": current_date},
                {"$set": update_doc}
            ))

            # Perform batch updates
            if len(bulk_operations) >= batch_size:
                ohlcv_collection.bulk_write(bulk_operations, ordered=False)
                bulk_operations.clear()

        if bulk_operations:
            ohlcv_collection.bulk_write(bulk_operations, ordered=False)

        return f"Successfully updated {ticker} - {len(df)} records"
    except Exception as e:
        return f"Error processing {ticker}: {str(e)}"

def calculate_rs_score_ranking():
    """
    Calculate and rank RS scores for all stocks based on their latest RS values.
    """
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

    # Sort stocks by RS value (using fallbacks)
    stocks.sort(key=lambda x: (
        x.get('RS4') or 
        x.get('RS3') or 
        x.get('RS2') or 
        x.get('RS1') or 
        float('-inf')
    ))

    total_stocks = len(stocks)
    if total_stocks == 0:
        logging.error("No stocks found with RS values")
        return

    # Calculate and update RS scores
    bulk_operations = []
    for rank, doc in enumerate(stocks, 1):
        ticker = doc['ticker']
        percentile_rank = (rank / total_stocks) * 100
        rs_score = max(1, min(99, round(percentile_rank)))

        bulk_operations.append(UpdateOne(
            {"ticker": ticker, "date": latest_date},
            {"$set": {"rs_score": rs_score}},
            upsert=True
        ))

        logging.info(f"Ticker: {ticker}, Rank: {rank}, RS Score: {rs_score}")

    # Execute bulk update
    if bulk_operations:
        indicators_collection.bulk_write(bulk_operations, ordered=False)

    logging.info("RS ranking and score calculation completed.")

def main():
    tickers = ohlcv_collection.distinct("ticker")
    total_tickers = len(tickers)

    logging.info(f"Starting bulk update for {total_tickers} tickers")

    # Use ThreadPoolExecutor with appropriate number of workers for parallel processing
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust workers based on resources
        futures = {executor.submit(calculate_rs_scores, ticker): ticker for ticker in tickers}

        for future in tqdm(as_completed(futures), total=total_tickers, desc="Processing tickers"):
            result = future.result()
            ticker = futures[future]
            if "Error" in result:
                logging.error(result)
            elif "No data" in result:
                logging.warning(result)
            else:
                logging.info(result)

    # Calculate RS score ranking after updating all tickers
    calculate_rs_score_ranking()

    logging.info("Bulk update and ranking complete")

if __name__ == "__main__":
    main()
