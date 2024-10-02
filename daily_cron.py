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

def fetch_todays_data():
    tickers = collection.distinct("ticker")
    logger.info(f"Fetching data for {len(tickers)} tickers.")

    # Get today's date in UK time
    uk_tz = pytz.timezone('Europe/London')
    today = datetime.now(uk_tz).date()

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            data = stock.history(period="1d", start=today, end=today + timedelta(days=1))

            if data.empty:
                logger.warning(f"No data found for {ticker} today, possibly due to a non-trading day.")
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
            logger.info(f"Updated {ticker} with today's data.")
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")

if __name__ == "__main__":
    fetch_todays_data()
