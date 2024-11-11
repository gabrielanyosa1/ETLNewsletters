import logging
from mongo_loader import MongoDBLoader
from pymongo.errors import ConnectionFailure
from datetime import datetime, timedelta
from collections import Counter
import re
from typing import List, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_keywords(subject: str) -> List[str]:
    """Extract meaningful keywords from subject line."""
    # Remove special characters and convert to lowercase
    cleaned = re.sub(r'[^\w\s]', ' ', subject.lower())
    words = cleaned.split()
    # Filter out common stop words
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
    return [word for word in words if word not in stop_words and len(word) > 2]

def analyze_daily_distribution(collection) -> Dict:
    """Analyze email distribution by day."""
    pipeline = [
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {
                            "$dateFromString": {
                                "dateString": "$parsedDate"
                            }
                        }
                    }
                },
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def analyze_content_length(collection) -> Dict:
    """Analyze email content length statistics."""
    pipeline = [
        {
            "$project": {
                "contentLength": {"$strLenCP": "$body.clean_text"},
                "urlCount": {"$size": {"$ifNull": ["$body.urls", []]}},
            }
        },
        {
            "$group": {
                "_id": None,
                "avgLength": {"$avg": "$contentLength"},
                "minLength": {"$min": "$contentLength"},
                "maxLength": {"$max": "$contentLength"},
                "totalUrls": {"$sum": "$urlCount"},
                "avgUrls": {"$avg": "$urlCount"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))[0]

def analyze_subject_keywords(collection) -> List[tuple]:
    """Analyze common keywords in subject lines."""
    subjects = collection.find({}, {"subject": 1})
    all_keywords = []
    for doc in subjects:
        if doc.get('subject'):
            all_keywords.extend(extract_keywords(doc['subject']))
    return Counter(all_keywords).most_common(10)

def verify_mongodb_data():
    """Verify and analyze the data loaded in MongoDB."""
    loader = None
    try:
        loader = MongoDBLoader()
        
        if not loader.connect():
            logger.error("Failed to connect to MongoDB")
            return
            
        loader.initialize_database()
        
        # Basic Collection Stats
        logger.info("\n=== Basic Collection Statistics ===")
        total_docs = loader.collection.count_documents({})
        logger.info(f"Total documents in collection: {total_docs}")
        
        # Date Range Analysis
        latest = list(loader.collection.find().sort("parsedDate", -1).limit(1))
        earliest = list(loader.collection.find().sort("parsedDate", 1).limit(1))
        
        if latest and earliest:
            logger.info(f"Date range: from {earliest[0].get('parsedDate')} to {latest[0].get('parsedDate')}")
        
        # Daily Distribution
        logger.info("\n=== Daily Email Distribution ===")
        daily_dist = analyze_daily_distribution(loader.collection)
        for day in daily_dist:
            logger.info(f"  {day['_id']}: {day['count']} emails")
            
        # Sender Analysis
        logger.info("\n=== Top Email Senders ===")
        pipeline = [
            {"$group": {"_id": "$from", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        
        top_senders = list(loader.collection.aggregate(pipeline))
        for sender in top_senders:
            logger.info(f"  {sender['_id']}: {sender['count']} emails")
            
        # Content Analysis
        logger.info("\n=== Content Analysis ===")
        content_stats = analyze_content_length(loader.collection)
        logger.info(f"Average content length: {int(content_stats['avgLength'])} characters")
        logger.info(f"Shortest email: {content_stats['minLength']} characters")
        logger.info(f"Longest email: {content_stats['maxLength']} characters")
        logger.info(f"Total URLs found: {content_stats['totalUrls']}")
        logger.info(f"Average URLs per email: {content_stats['avgUrls']:.2f}")
            
        # Subject Analysis
        logger.info("\n=== Top Subject Keywords ===")
        keywords = analyze_subject_keywords(loader.collection)
        for keyword, count in keywords:
            logger.info(f"  {keyword}: {count} occurrences")
            
        # Sample Content
        logger.info("\n=== Sample Email Subjects ===")
        subjects = list(loader.collection.find({}, {"subject": 1}).limit(5))
        for subject in subjects:
            logger.info(f"  - {subject.get('subject', 'No subject')}")
            
        # Data Quality Checks
        logger.info("\n=== Data Quality Checks ===")
        missing_dates = loader.collection.count_documents({"parsedDate": None})
        missing_bodies = loader.collection.count_documents({"body": None})
        missing_subjects = loader.collection.count_documents({"subject": None})
        
        if missing_dates > 0:
            logger.warning(f"Found {missing_dates} documents with missing dates")
        if missing_bodies > 0:
            logger.warning(f"Found {missing_bodies} documents with missing body content")
        if missing_subjects > 0:
            logger.warning(f"Found {missing_subjects} documents with missing subjects")
            
        # Check for duplicate IDs
        pipeline = [
            {"$group": {"_id": "$id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        duplicates = list(loader.collection.aggregate(pipeline))
        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate IDs")
            
    except ConnectionFailure as e:
        logger.error(f"MongoDB connection error: {e}")
    except Exception as e:
        logger.error(f"Error during verification: {e}")
    finally:
        if loader:
            loader.close()

if __name__ == "__main__":
    verify_mongodb_data()