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
import warnings
from functools import wraps

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def deprecated(func):
    """Decorator to mark functions as deprecated."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        warnings.warn(
            f"Call to deprecated function {func.__name__}. "
            f"Use {func.__name__.replace('emails', 'data')} instead.",
            category=DeprecationWarning,
            stacklevel=2
        )
        return func(*args, **kwargs)
    return wrapper

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
            logger.info("Attempting to connect to MongoDB...")
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))
            # Test connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            return True
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {str(e)}")
            return False

    def initialize_database(self, db_name: str = 'gmail_archive', 
                          collection_name: str = 'emails') -> None:
        """Initialize database and collection."""
        try:
            if self.client is None:
                logger.error("Cannot initialize database: No MongoDB connection available")
                raise ConnectionError("No MongoDB connection available")
            
            logger.info(f"Initializing database '{db_name}' and collection '{collection_name}'")
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            
            # Create indexes
            logger.info("Creating indexes...")
            index_results = []
            index_results.append(self.collection.create_index([('id', 1)], unique=True))
            index_results.append(self.collection.create_index([('parsedDate', -1)]))
            index_results.append(self.collection.create_index([('from', 1)]))
            index_results.append(self.collection.create_index([('subject', 1)]))
            
            logger.info(f"Successfully created indexes: {index_results}")
            logger.info(f"Collection stats: {self.db.command('collstats', collection_name)}")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}", exc_info=True)
            raise

    def load_data(self, data, batch_size: int = 100) -> Dict:
        """
        Load email data into MongoDB.
        """
        stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'duplicates': 0,
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'errors': []  # Track specific errors
        }
        
        try:
            # Handle input data
            if isinstance(data, str):
                logger.info(f"Reading email data from file: {data}")
                with open(data, 'r', encoding='utf-8') as f:
                    emails = json.load(f)
            elif isinstance(data, list):
                logger.info(f"Processing provided email list (length: {len(data)})")
                emails = data
            else:
                raise ValueError(f"Invalid data type: {type(data)}. Must be str or list.")

            # Log sample of first email for debugging
            if emails:
                logger.debug(f"Sample email structure: {list(emails[0].keys())}")
                
            # Initialize database
            logger.info("Initializing database...")
            self.initialize_database()
            logger.info("Database initialization complete")
                
            # Process emails in batches
            total_batches = len(emails) // batch_size + (1 if len(emails) % batch_size else 0)
            logger.info(f"Processing {len(emails)} emails in {total_batches} batches (size: {batch_size})")
            
            batch = []
            current_batch = 0
            
            for idx, email in enumerate(emails):
                try:
                    # Add metadata
                    email['_imported_at'] = datetime.now().isoformat()
                    batch.append(email)
                    stats['total_processed'] += 1
                    
                    # Process batch
                    if len(batch) >= batch_size:
                        current_batch += 1
                        logger.info(f"Processing batch {current_batch}/{total_batches}")
                        self._process_batch(batch, stats)
                        batch = []
                        
                except Exception as e:
                    error_msg = f"Error processing email {idx}: {str(e)}"
                    logger.error(error_msg)
                    stats['errors'].append(error_msg)
                    stats['failed'] += 1
                    
            # Process remaining emails
            if batch:
                current_batch += 1
                logger.info(f"Processing final batch {current_batch}/{total_batches}")
                self._process_batch(batch, stats)
                
            stats['end_time'] = datetime.now().isoformat()
            
            # Log final statistics
            logger.info("Email loading completed. Final stats:")
            logger.info(f"  Total processed: {stats['total_processed']}")
            logger.info(f"  Successful: {stats['successful']}")
            logger.info(f"  Failed: {stats['failed']}")
            logger.info(f"  Duplicates: {stats['duplicates']}")
            if stats['errors']:
                logger.info("Errors encountered:")
                for error in stats['errors'][:5]:  # Show first 5 errors
                    logger.info(f"  - {error}")
            
            return stats
            
        except Exception as e:
            error_msg = f"Failed to load emails: {str(e)}"
            logger.error(error_msg, exc_info=True)
            stats['errors'].append(error_msg)
            stats['end_time'] = datetime.now().isoformat()
            return stats
    
    def get_collection_stats(self) -> Dict:
        """Get current statistics of the MongoDB collection."""
        try:
            if self.collection is None:  # Correct comparison with None
                self.initialize_database()

            stats = {
                'total_documents': self.collection.count_documents({}),
                'date_range': {
                    'earliest': None,
                    'latest': None
                },
                'sender_counts': {}
            }
            
            # Get date range
            latest = list(self.collection.find({}, {'parsedDate': 1})
                        .sort('parsedDate', -1).limit(1))
            earliest = list(self.collection.find({}, {'parsedDate': 1})
                        .sort('parsedDate', 1).limit(1))
            
            if latest and earliest:
                stats['date_range']['latest'] = latest[0].get('parsedDate')
                stats['date_range']['earliest'] = earliest[0].get('parsedDate')
                
            # Get sender distribution
            pipeline = [
                {"$group": {"_id": "$from", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5}
            ]
            
            for doc in self.collection.aggregate(pipeline):
                stats['sender_counts'][doc['_id']] = doc['count']
                
            return stats
        except Exception as e:
            logger.error(f"Error getting collection stats: {e}")
            return None
    
    def _process_batch(self, batch: List[Dict], stats: Dict) -> None:
        """Process a batch of emails."""
        try:
            logger.debug(f"Processing batch of {len(batch)} emails")
            result = self.collection.insert_many(batch, ordered=False)
            inserted_count = len(result.inserted_ids)
            stats['successful'] += inserted_count
            logger.info(f"Successfully inserted {inserted_count} documents")
            
        except OperationFailure as e:
            if "duplicate key error" in str(e):
                # Get detailed error information
                if hasattr(e, 'details'):
                    successful = e.details.get('nInserted', 0)
                    write_errors = e.details.get('writeErrors', [])
                    stats['successful'] += successful
                    stats['duplicates'] += len(batch) - successful
                    
                    # Log specific duplicate key errors
                    for error in write_errors[:5]:  # Log first 5 errors
                        logger.warning(f"Duplicate key error: {error.get('errmsg', 'Unknown error')}")
                else:
                    stats['duplicates'] += len(batch)
                logger.warning(f"Batch processing completed with duplicates. Successful: {stats['successful']}, Duplicates: {stats['duplicates']}")
            else:
                error_msg = f"Batch processing error: {str(e)}"
                logger.error(error_msg)
                stats['errors'].append(error_msg)
                stats['failed'] += len(batch)
                
        except Exception as e:
            error_msg = f"Unexpected error in batch processing: {str(e)}"
            logger.error(error_msg, exc_info=True)
            stats['errors'].append(error_msg)
            stats['failed'] += len(batch)

    def close(self):
        if self.client is not None:
            logger.info("Closing MongoDB connection")
            self.client.close()
            self.client = None
            self.db = None
            self.collection = None
            logger.info("MongoDB connection closed")
    
    @deprecated
    def load_emails(self, data, batch_size: int = 100) -> Dict:
        """
        Deprecated: Use load_data() instead.
        
        This method is maintained for backward compatibility and will be removed in a future version.
        """
        return self.load_data(data, batch_size)


if __name__ == "__main__":
    # Example usage
    loader = MongoDBLoader()
    if loader.connect():
        try:
            stats = loader.load_emails('filtered_emails.json')
            print(f"Import stats: {stats}")
        finally:
            loader.close()