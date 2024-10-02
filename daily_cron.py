from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz
import yfinance as yf
import logging

# MongoDB connection (hardcoded)
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
collection = db['ohlcv_data']

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_yesterdays_data():
    tickers = collection.distinct("ticker")
    logger.info(f"Fetching data for {len(tickers)} tickers.")

    # Get yesterday's date in UK time
    uk_tz = pytz.timezone('Europe/London')
    yesterday = datetime.now(uk_tz).date() - timedelta(days=1)
    logger.info(f"Attempting to fetch data for date: {yesterday}")

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            
            # Add logging to see what dates are being requested
            logger.info(f"Fetching data for {ticker} from {yesterday} to {yesterday + timedelta(days=1)}")

            # Request data for the past week to debug if data fetching works
            data = stock.history(period="5d")

            if data.empty:
                logger.warning(f"No data found for {ticker}, possibly delisted or unavailable.")
                continue  # Skip to the next ticker

            # Safely access the data row
            row = data.iloc[0]
            record = {
                'ticker': ticker,
                'date': row.name.to_pydatetime(),
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume'],
            }
            collection.update_one({'ticker': ticker, 'date': record['date']}, {'$set': record}, upsert=True)
            logger.info(f"Updated {ticker} with data.")
        except IndexError as e:
            logger.error(f"Error fetching data for {ticker}: {e} - No data available.")
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")

if __name__ == "__main__":
    fetch_yesterdays_data()

    # Commenting out notify_status for now to avoid errors
    # notify_status(True)
