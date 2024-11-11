import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.collection import Collection
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MongoDBLoader:
    def __init__(self, connection_string: Optional[str] = None):
        load_dotenv()
        self.uri = connection_string or os.getenv('MONGODB_URI')
        if not self.uri:
            raise ValueError("MongoDB connection URI not provided and not found in environment")
        self.client = None
        self.db = None
        self.collection = None
        
    def connect(self) -> bool:
        try:
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))
            # Test connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            return True
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False

    def initialize_database(self, db_name: str = 'gmail_archive', 
                          collection_name: str = 'emails') -> None:
        """Initialize database and collection."""
        try:
            if self.client is None:
                raise ConnectionError("No MongoDB connection available")
                
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            
            # Create indexes
            self.collection.create_index([('id', 1)], unique=True)
            self.collection.create_index([('parsedDate', -1)])
            self.collection.create_index([('from', 1)])
            self.collection.create_index([('subject', 1)])
            
            logger.info(f"Initialized database '{db_name}' and collection '{collection_name}'")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    def load_emails(self, filepath: str, batch_size: int = 100) -> Dict:
        stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'duplicates': 0,
            'start_time': datetime.now().isoformat(),
            'end_time': None
        }
        
        try:
            # Read JSON file
            logger.info(f"Reading email data from {filepath}")
            with open(filepath, 'r', encoding='utf-8') as f:
                emails = json.load(f)
                
            if not isinstance(emails, list):
                raise ValueError("JSON file must contain a list of email objects")
                
            # Initialize database
            self.initialize_database()
            
            # Process emails in batches
            batch = []
            
            for email in emails:
                try:
                    # Add metadata
                    email['_imported_at'] = datetime.now().isoformat()
                    batch.append(email)
                    stats['total_processed'] += 1
                    
                    # Process batch
                    if len(batch) >= batch_size:
                        self._process_batch(batch, stats)
                        batch = []
                        
                except Exception as e:
                    logger.error(f"Error processing email: {str(e)}")
                    stats['failed'] += 1
                    
            # Process remaining emails
            if batch:
                self._process_batch(batch, stats)
                
            stats['end_time'] = datetime.now().isoformat()
            logger.info(f"Email loading completed. Stats: {stats}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to load emails: {str(e)}")
            stats['end_time'] = datetime.now().isoformat()
            return stats

    def _process_batch(self, batch: List[Dict], stats: Dict) -> None:
        """Process a batch of emails."""
        try:
            result = self.collection.insert_many(batch, ordered=False)
            inserted_count = len(result.inserted_ids)
            stats['successful'] += inserted_count
            logger.info(f"Successfully inserted {inserted_count} documents")
            
        except OperationFailure as e:
            if "duplicate key error" in str(e):
                if hasattr(e, 'details'):
                    successful = e.details.get('nInserted', 0)
                    stats['successful'] += successful
                    stats['duplicates'] += len(batch) - successful
                else:
                    stats['duplicates'] += len(batch)
                logger.warning(f"Found duplicate documents. Successful: {successful}, Duplicates: {stats['duplicates']}")
            else:
                logger.error(f"Batch processing error: {str(e)}")
                stats['failed'] += len(batch)
                
        except Exception as e:
            logger.error(f"Unexpected error in batch processing: {str(e)}")
            stats['failed'] += len(batch)

    def close(self):
        if self.client is not None:
            self.client.close()
            logger.info("Closed MongoDB connection")


if __name__ == "__main__":
    # Example usage
    loader = MongoDBLoader()
    if loader.connect():
        try:
            stats = loader.load_emails('filtered_emails.json')
            print(f"Import stats: {stats}")
        finally:
            loader.close()