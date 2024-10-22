from pymongo import MongoClient
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection setup
client = MongoClient("mongodb://mongodb-9iyq:27017")
db = client['StockData']
indicators_collection = db['indicators']

def cleanup_indicators():
    """
    Retain only the records in the 'indicators' collection that have 'sector' and 'industry' fields.
    Delete all other records that do not contain this information.
    """
    try:
        # Define the criteria for keeping records: they must have both 'sector' and 'industry' fields
        keep_criteria = {"sector": {"$exists": True, "$ne": None}, "industry": {"$exists": True, "$ne": None}}
        
        # Count the number of documents that will be retained
        to_keep_count = indicators_collection.count_documents(keep_criteria)
        logging.info(f"Number of documents to retain: {to_keep_count}")
        
        # Delete all records that do not match the keep criteria
        result = indicators_collection.delete_many({
            "$or": [
                {"sector": {"$exists": False}},
                {"industry": {"$exists": False}}
            ]
        })
        
        logging.info(f"Deleted {result.deleted_count} documents from the indicators collection.")
        logging.info("Cleanup process completed.")
    
    except Exception as e:
        logging.error(f"Error during cleanup: {str(e)}")

if __name__ == "__main__":
    cleanup_indicators()

