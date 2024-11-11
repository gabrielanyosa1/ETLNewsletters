import json
import logging
from mongo_loader import MongoDBLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_state():
    """Verify the state of JSON and MongoDB data."""
    try:
        # Check JSON file
        with open('filtered_emails.json', 'r') as f:
            json_data = json.load(f)
        logger.info(f"JSON file contains {len(json_data)} emails")
        
        # Check MongoDB
        mongo_loader = MongoDBLoader()
        if mongo_loader.connect():
            stats = mongo_loader.get_collection_stats()
            logger.info("\nMongoDB Stats:")
            logger.info(f"Total documents: {stats['total_documents']}")
            logger.info(f"Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
            logger.info("\nTop senders:")
            for sender, count in stats['sender_counts'].items():
                logger.info(f"  {sender}: {count}")
                
            # Compare IDs
            json_ids = set(email['id'] for email in json_data)
            mongo_ids = set(doc['id'] for doc in mongo_loader.collection.find({}, {'id': 1}))
            
            logger.info("\nComparison:")
            logger.info(f"IDs in JSON but not in MongoDB: {len(json_ids - mongo_ids)}")
            logger.info(f"IDs in MongoDB but not in JSON: {len(mongo_ids - json_ids)}")
            
            if json_ids != mongo_ids:
                logger.warning("Data inconsistency detected!")
                
        mongo_loader.close()
        
    except Exception as e:
        logger.error(f"Error during verification: {e}")

if __name__ == "__main__":
    verify_state()