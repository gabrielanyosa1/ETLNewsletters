import os
import sys
from pathlib import Path
import datetime
import json
import logging
from logging.handlers import RotatingFileHandler
from dateutil import parser
import platform
from mongo_loader import MongoDBLoader
from analysis.eda.subject_analyzer import EmailSubjectAnalyzer
from analysis.eda.grafana_publisher import GrafanaPublisher

# Get the absolute path of the project root
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))

# Create logs directory
log_dir = project_root / "logs"
log_dir.mkdir(exist_ok=True)

class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for logging."""
    def format(self, record):
        log_obj = {
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)

def setup_logging():
    """Configure logging to both console and JSON file."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Console handler with standard formatting
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )
    logger.addHandler(console_handler)

    # JSON file handler with rotation
    json_handler = RotatingFileHandler(
        log_dir / "analysis.json",
        maxBytes=1024*1024,  # 1MB
        backupCount=5
    )
    json_handler.setFormatter(JsonFormatter())
    logger.addHandler(json_handler)
    
    return logger

logger = setup_logging()

def parse_timestamp(timestamp_str):
    """Safely parse ISO format timestamps."""
    if not timestamp_str:
        return None
        
    try:
        return parser.parse(timestamp_str)
    except Exception as e:
        logger.error(f"Error parsing timestamp {timestamp_str}: {str(e)}")
        return None

def format_timestamp(dt):
    """Format datetime object to ISO 8601 string."""
    if not dt:
        return None
    try:
        return dt.isoformat()
    except Exception as e:
        logger.error(f"Error formatting datetime {dt}: {str(e)}")
        return None

def main():
    mongo_loader = None
    try:
        # Initialize MongoDB connection
        mongo_loader = MongoDBLoader()
        if not mongo_loader.connect():
            logger.error("Failed to connect to MongoDB")
            return

        # Initialize the database
        mongo_loader.initialize_database()
        
        # Initialize analyzer and publisher
        analyzer = EmailSubjectAnalyzer(mongo_loader)
        publisher = GrafanaPublisher()

        # Run analysis
        logger.info("Starting analysis...")
        analysis_data = analyzer.run_analysis()
        
        if analysis_data:
            logger.info("Analysis completed, publishing to Grafana...")
            result = publisher.create_email_analysis_dashboard(analysis_data)
            logger.info("Published to Grafana successfully")
        else:
            logger.error("Analysis produced no data")

    except Exception as e:
        logger.error(f"Error in analysis pipeline: {str(e)}")
        raise
    finally:
        if mongo_loader:
            logger.info("Closing MongoDB connection")
            mongo_loader.close()
if __name__ == "__main__":
    main()