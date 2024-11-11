# test_mongo_load.py
import logging
from mongo_loader import MongoDBLoader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_mongodb_load():
    """Test loading existing JSON data into MongoDB."""
    try:
        # Initialize loader
        loader = MongoDBLoader()
        
        # Connect to MongoDB
        if loader.connect():
            # Load the existing JSON file
            stats = loader.load_emails('filtered_emails.json')
            logger.info(f"Import stats: {stats}")
        else:
            logger.error("Failed to connect to MongoDB")
            
    except Exception as e:
        logger.error(f"Error during MongoDB test: {e}")
        
    finally:
        if loader:
            loader.close()

if __name__ == "__main__":
    test_mongodb_load()
