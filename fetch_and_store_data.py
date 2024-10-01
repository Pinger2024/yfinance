import yfinance as yf
from pymongo import MongoClient
import pandas as pd
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection (replace with your credentials)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
ohlcv_collection = db['ohlcv_data']
meta_collection = db['meta_data']

# Create a unique index for ohlcv collection to avoid duplicates
ohlcv_collection.create_index([('ticker', 1), ('date', 1)], unique=True)

# Track success and failures
successful_tickers = []
failed_tickers = []

# Function to fetch OHLCV data and store it in MongoDB
def fetch_and_store_ticker_data(ticker):
    logging.info(f"Fetching data for {ticker}")
    stock = yf.Ticker(ticker)

    try:
        # Fetch historical OHLCV data
        hist = stock.history(period="2y")

        # Skip if no data found
        if hist.empty:
            logging.warning(f"No data found for {ticker}, possibly delisted or unavailable.")
            failed_tickers.append(ticker)  # Mark as failed
            return False

        # Store the OHLCV data in MongoDB
        for date, row in hist.iterrows():
            data = {
                'ticker': ticker,
                'date': date,
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume']
            }
            try:
                ohlcv_collection.update_one(
                    {'ticker': ticker, 'date': data['date']},
                    {'$set': data},
                    upsert=True
                )
            except Exception as e:
                logging.error(f"Error inserting data for {ticker} on {date}: {e}")
                failed_tickers.append(ticker)  # Mark as failed if insertion fails
                return False

        logging.info(f"Successfully stored data for {ticker}")
        successful_tickers.append(ticker)  # Mark as success
        return True

    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        failed_tickers.append(ticker)  # Mark as failed
        return False

# Rate-limited process to fetch data in batches
def fetch_data_in_batches(tickers, batch_size=100, delay=60):
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    for i in range(0, len(tickers), batch_size):
        batch_number = i // batch_size + 1
        batch = tickers[i:i + batch_size]
        logging.info(f"Processing batch {batch_number}/{total_batches}")
        for ticker in batch:
            success = fetch_and_store_ticker_data(ticker)
            if not success:
                logging.warning(f"Encountered error with {ticker}, skipping for now.")
        if i + batch_size < len(tickers):
            logging.info(f"Batch {batch_number} completed, sleeping for {delay} seconds...")
            time.sleep(delay)
    logging.info("All batches processed.")

    # Summary message at the end
    logging.info("\n\n=== SUMMARY ===")
    logging.info(f"Total tickers expected: {len(tickers)}")
    logging.info(f"Successfully fetched data for: {len(successful_tickers)} tickers")
    logging.info(f"Failed to fetch data for: {len(failed_tickers)} tickers")

    # Print failed tickers for easier debugging
    if failed_tickers:
        logging.info(f"Failed tickers: {', '.join(failed_tickers)}")
    else:
        logging.info("No tickers failed.")

if __name__ == "__main__":
    # Load tickers from CSV files
    us_stocks = pd.read_csv('path_to_us_csv')['Symbol']
    uk_stocks = pd.read_csv('path_to_uk_csv')['Symbol']

    # Combine and drop duplicates
    all_tickers = pd.concat([us_stocks, uk_stocks]).drop_duplicates().tolist()

    # Fetch data in batches
    fetch_data_in_batches(all_tickers)
