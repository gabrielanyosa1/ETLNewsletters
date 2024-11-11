import json
import logging
from mongo_loader import MongoDBLoader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def sync_mongodb():
    """Synchronize MongoDB with the JSON file."""
    try:
        # Load JSON data
        with open('filtered_emails.json', 'r') as f:
            json_data = json.load(f)
        logger.info(f"Loaded {len(json_data)} emails from JSON")
        
        # Connect to MongoDB
        mongo_loader = MongoDBLoader()
        if not mongo_loader.connect():
            logger.error("Failed to connect to MongoDB")
            return
            
        try:
            # Get current MongoDB state
            before_stats = mongo_loader.get_collection_stats()
            if before_stats:
                logger.info(f"Current MongoDB documents: {before_stats['total_documents']}")
                
                # Get existing IDs in MongoDB
                existing_ids = set(doc['id'] for doc in mongo_loader.collection.find({}, {'id': 1}))
                logger.info(f"Found {len(existing_ids)} existing IDs in MongoDB")
                
                # Find missing documents
                json_ids = set(email['id'] for email in json_data)
                missing_ids = json_ids - existing_ids
                logger.info(f"Found {len(missing_ids)} missing documents")
                
                if missing_ids:
                    # Filter JSON data to only include missing documents
                    missing_docs = [email for email in json_data if email['id'] in missing_ids]
                    logger.info(f"Attempting to insert {len(missing_docs)} missing documents")
                    
                    # Insert missing documents
                    stats = mongo_loader.load_data(missing_docs)
                    logger.info(f"Sync stats: {stats}")
                    
                    # Verify final state
                    after_stats = mongo_loader.get_collection_stats()
                    if after_stats:
                        logger.info(f"Final MongoDB documents: {after_stats['total_documents']}")
                        if after_stats['total_documents'] == len(json_data):
                            logger.info("Sync successful! MongoDB is now in sync with JSON")
                        else:
                            logger.warning("Sync completed but counts still don't match")
                            logger.warning(f"JSON: {len(json_data)}, MongoDB: {after_stats['total_documents']}")
                else:
                    logger.info("No missing documents found")
            
        finally:
            mongo_loader.close()
            
    except Exception as e:
        logger.error(f"Error during sync: {e}", exc_info=True)

if __name__ == "__main__":
    sync_mongodb()