from flask import Flask, render_template
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection
client = MongoClient("mongodb://mongodb-9iyq:27017")  # Ensure this is correct for your MongoDB instance
db = client['StockData']

# Collections for OHLCV data and Meta Data
ohlcv_collection = db['ohlcv_data']
meta_data_collection = db['meta_data']

@app.route('/')
def index():
    # Query the OHLCV collection for the first 5 tickers' daily data
    stock_data = list(ohlcv_collection.find().limit(5))
    print("Stock Data:", stock_data)  # Debugging: Print the fetched stock data

    # Ensure stock data is found
    if stock_data:
        # Count the number of distinct tickers in the OHLCV collection
        unique_tickers_count = len(ohlcv_collection.distinct('ticker'))
        print("Unique Tickers Count:", unique_tickers_count)  # Debugging: Print the count
    else:
        unique_tickers_count = 0

    # Optionally, retrieve some meta data (e.g., financials) for the first ticker
    meta_data = meta_data_collection.find_one({'ticker': stock_data[0]['ticker']}) if stock_data else {}
    print("Meta Data:", meta_data)  # Debugging: Print the fetched meta data

    # Pass the OHLCV data, meta data, and ticker count to the template
    return render_template('index.html', stocks=stock_data, meta_data=meta_data, tickers_count=unique_tickers_count)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
