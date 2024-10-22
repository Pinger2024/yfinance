from pymongo import MongoClient, UpdateOne
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
indicators_collection = db['indicators']

def calculate_rs_scores_for_group(stocks, field_name):
    """
    Calculate RS scores for a group (sector or industry).
    """
    # Extract RS weighted scores for all stocks in the group
    scores = [stock['rs_weighted_score'] for stock in stocks if 'rs_weighted_score' in stock]
    
    # Calculate ranks based on the weighted scores
    percentile_ranks = pd.Series(scores).rank(pct=True)
    
    # Convert to 1-99 range
    normalized_scores = (percentile_ranks * 98 + 1).round().astype(int)
    
    # Update the database with sector or industry RS scores
    bulk_operations = []
    for stock, normalized in zip(stocks, normalized_scores):
        bulk_operations.append(
            UpdateOne(
                {"_id": stock["_id"]},
                {"$set": {field_name: int(normalized)}}
            )
        )
    
    # Execute bulk write
    if bulk_operations:
        indicators_collection.bulk_write(bulk_operations, ordered=False)
        logging.info(f"Updated {len(bulk_operations)} documents with {field_name}")

def calculate_sector_industry_rs_scores():
    """
    Calculate RS scores for stocks within their sector and industry.
    """
    # Group by sector
    sectors = indicators_collection.aggregate([
        {"$group": {"_id": "$sector", "stocks": {"$push": "$$ROOT"}}}
    ])
    
    # Calculate RS scores within each sector
    for sector in sectors:
        logging.info(f"Processing sector: {sector['_id']}")
        calculate_rs_scores_for_group(sector['stocks'], "sector_rs_score")
    
    # Group by industry
    industries = indicators_collection.aggregate([
        {"$group": {"_id": "$industry", "stocks": {"$push": "$$ROOT"}}}
    ])
    
    # Calculate RS scores within each industry
    for industry in industries:
        logging.info(f"Processing industry: {industry['_id']}")
        calculate_rs_scores_for_group(industry['stocks'], "industry_rs_score")

if __name__ == "__main__":
    calculate_sector_industry_rs_scores()
