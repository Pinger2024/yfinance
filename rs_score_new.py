import pandas as pd
from pymongo import MongoClient, UpdateOne
import logging
from datetime import datetime
import warnings
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']

def calculate_rs_scores(ticker):
    try:
        # Get all historical data for the ticker
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

        periods = {
            "RS1": 63,
            "RS2": 126,
            "RS3": 189,
            "RS4": 252
        }

        # Compute RS values using shifted close prices
        for rs_key, period in periods.items():
            df[f'close_shift_{period}'] = df['close'].shift(period)
            df[rs_key] = (df['close'] - df[f'close_shift_{period}']) / df[f'close_shift_{period}'] * 100

        # Drop rows where 'daily_pct_change' is NaN (first row)
        df = df.dropna(subset=['daily_pct_change']).reset_index(drop=True)

        # Prepare bulk operations
        bulk_operations = []
        batch_size = 100  # Control the size of batches to avoid memory issues

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

def main():
    tickers = ohlcv_collection.distinct("ticker")
    total_tickers = len(tickers)

    logging.info(f"Starting bulk update for {total_tickers} tickers")

    # Use ThreadPoolExecutor with appropriate number of workers
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

    logging.info("Bulk update complete")

if __name__ == "__main__":
    main()
