import pandas as pd
from pymongo import MongoClient, UpdateOne
import logging
from datetime import datetime
import numpy as np

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RSScoreCalculator:
    def __init__(self, mongodb_uri="mongodb://mongodb-9iyq:27017", db_name='StockData'):
        """Initialize the RS Score Calculator with MongoDB connection."""
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[db_name]
        self.ohlcv_collection = self.db['ohlcv_data']
        self.indicators_collection = self.db['indicators']
        
    def calculate_weighted_rs_score(self, ticker):
        """
        Calculate weighted RS score for a stock using multiple timeframes.
        Returns both raw and weighted scores for transparency.
        """
        try:
            # Get the latest date with RS values
            latest_date = self.ohlcv_collection.find_one(
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
                logging.warning(f"No RS values found for {ticker}")
                return None
                
            latest_date = latest_date["date"]
            
            # Fetch RS values
            rs_data = self.ohlcv_collection.find_one(
                {"ticker": ticker, "date": latest_date},
                {"RS1": 1, "RS2": 1, "RS3": 1, "RS4": 1}
            )
            
            if not rs_data:
                logging.warning(f"No RS data for {ticker} on {latest_date}")
                return None
            
            # Define weights for different timeframes
            weights = {
                "RS1": 0.40,  # 40% weight for 3-month RS
                "RS2": 0.30,  # 30% weight for 6-month RS
                "RS3": 0.20,  # 20% weight for 9-month RS
                "RS4": 0.10   # 10% weight for 12-month RS
            }
            
            # Calculate weighted score
            weighted_score = sum(
                weights[f"RS{i}"] * rs_data.get(f"RS{i}", 0)
                for i in range(1, 5)
            )
            
            # Store raw RS values for transparency
            raw_scores = {
                f"RS{i}": rs_data.get(f"RS{i}", 0)
                for i in range(1, 5)
            }
            
            return {
                "ticker": ticker,
                "date": latest_date,
                "weighted_score": weighted_score,
                "raw_scores": raw_scores
            }
            
        except Exception as e:
            logging.error(f"Error calculating RS score for {ticker}: {str(e)}")
            return None

    def normalize_scores(self, scores):
        """
        Normalize RS scores to a 1-99 range using percentile ranking.
        This ensures an even distribution across the range, handling non-finite values.
        """
        if not scores:
            return []
            
        # Extract weighted scores and filter out invalid ones (e.g., NaN, inf)
        weighted_scores = [s['weighted_score'] for s in scores if np.isfinite(s['weighted_score'])]
        
        if len(weighted_scores) == 0:
            logging.error("No valid weighted scores available for normalization")
            return []
        
        # Calculate percentile ranks (0 to 1) only for valid scores
        percentile_ranks = pd.Series(weighted_scores).rank(pct=True)
        
        # Convert to 1-99 range
        normalized_scores = (percentile_ranks * 98 + 1).round().astype(int)
        
        # Update scores with normalized values and handle non-finite cases
        valid_scores = [s for s in scores if np.isfinite(s['weighted_score'])]
        for score, normalized in zip(valid_scores, normalized_scores):
            score['rs_score'] = normalized
            
        return valid_scores

    def update_database(self, scores):
        """Update the database with new RS scores and ranks."""
        if not scores:
            return
            
        # Sort by RS score in descending order
        scores_sorted = sorted(scores, key=lambda x: x['rs_score'], reverse=True)
        
        # Prepare bulk operations
        bulk_operations = []
        for rank, score in enumerate(scores_sorted, 1):
            bulk_operations.append(
                UpdateOne(
                    {
                        "ticker": score['ticker'],
                        "date": score['date']
                    },
                    {
                        "$set": {
                            "rs_score": score['rs_score'],
                            "rs_rank": rank
                        }
                    },
                    upsert=True
                )
            )
        
        # Execute bulk update
        if bulk_operations:
            result = self.indicators_collection.bulk_write(bulk_operations, ordered=False)
            logging.info(f"Updated {result.modified_count} documents")

    def calculate_all_scores(self):
        """Calculate and update RS scores for all stocks."""
        try:
            # Get all tickers
            tickers = self.ohlcv_collection.distinct("ticker")
            logging.info(f"Processing {len(tickers)} tickers")
            
            # Calculate weighted scores
            scores = []
            for ticker in tickers:
                result = self.calculate_weighted_rs_score(ticker)
                if result:
                    scores.append(result)
            
            # Normalize scores
            scores_normalized = self.normalize_scores(scores)
            
            # Update database
            self.update_database(scores_normalized)
            
            logging.info("RS score calculation completed successfully")
            
        except Exception as e:
            logging.error(f"Error in calculate_all_scores: {str(e)}")

if __name__ == "__main__":
    calculator = RSScoreCalculator()
    calculator.calculate_all_scores()
