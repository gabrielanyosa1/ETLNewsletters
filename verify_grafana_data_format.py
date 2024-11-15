from analysis.eda.subject_analyzer import EmailSubjectAnalyzer
from mongo_loader import MongoDBLoader
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_data_format():
    """Test the format of analyzed data for Grafana compatibility."""
    try:
        # Initialize MongoDB and analyzer
        mongo_loader = MongoDBLoader()
        if not mongo_loader.connect():
            logger.error("Failed to connect to MongoDB")
            return
            
        analyzer = EmailSubjectAnalyzer(mongo_loader)
        
        # Get analysis data
        data = analyzer.run_analysis()
        
        # Verify data structure
        logger.info("\nData Structure Verification:")
        logger.info("1. Time Series Data:")
        logger.info(f"   - Points: {len(data['time_series'])}")
        if data['time_series']:
            logger.info(f"   - Sample: {json.dumps(data['time_series'][0], indent=2)}")
            
        logger.info("\n2. Category Totals:")
        logger.info(f"   - Categories: {json.dumps(data['category_totals'], indent=2)}")
        
        logger.info("\n3. Top Themes:")
        logger.info(f"   - Themes: {json.dumps(dict(list(data['top_themes'].items())[:5]), indent=2)}")
        
        return data
        
    except Exception as e:
        logger.error(f"Error testing data format: {str(e)}")
        raise
    finally:
        if mongo_loader:
            mongo_loader.close()

if __name__ == "__main__":
    test_data_format()