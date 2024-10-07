import csv
import requests
from pymongo import MongoClient
import os

# MongoDB connection setup
try:
    mongo_uri = os.environ.get('MONGO_URI', 'mongodb://mongodb-9iyq:27017')
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client['StockData']
    indicators_collection = db['indicators']
    print("Successfully connected to MongoDB.")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    client = None

# Function to download CSV from GitHub
def download_csv_from_github(url, local_filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_filename, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded CSV to {local_filename}")
        return True
    else:
        print(f"Failed to download CSV. Status code: {response.status_code}")
        return False

# Function to update sector and industry in the indicators collection
def update_sector_and_industry(csv_file_path):
    if client is None:
        print("Unable to connect to the database.")
        return

    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            ticker = row['Symbol']
            sector = row['Sector']
            industry = row['Industry']

            if ticker:
                try:
                    # Update the indicators collection with sector and industry info
                    result = indicators_collection.update_one(
                        {"ticker": ticker},
                        {"$set": {"sector": sector, "industry": industry}}
                    )
                    if result.matched_count > 0:
                        print(f"Updated {ticker} with sector {sector} and industry {industry}")
                    else:
                        print(f"No matching ticker found for {ticker}")
                except Exception as e:
                    print(f"Error updating {ticker}: {e}")

if __name__ == "__main__":
    # GitHub raw CSV URL
    github_csv_url = 'https://raw.githubusercontent.com/Pinger2024/yfinance/main/sector_detail.csv'
    
    # Local file path to save the downloaded CSV
    local_csv_path = 'sector_detail.csv'

    # Download the CSV file from GitHub
    if download_csv_from_github(github_csv_url, local_csv_path):
        # Update the MongoDB records with sector and industry info
        update_sector_and_industry(local_csv_path)
