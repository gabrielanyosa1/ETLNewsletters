from analysis.eda.grafana_publisher import GrafanaPublisher
from mongo_loader import MongoDBLoader
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_grafana_connection():
    """Test Grafana Cloud connection and dashboard creation."""
    try:
        # Initialize publisher
        publisher = GrafanaPublisher()
        
        # Create test dashboard with sample data
        test_data = {
            "categories": ["crypto", "tech", "finance", "macro"],
            "regions": ["north_america", "europe", "asia", "latin_america"]
        }
        
        result = publisher.create_email_analysis_dashboard(test_data)
        logger.info(f"Successfully created test dashboard: {result['url']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to test Grafana connection: {str(e)}")
        return False

if __name__ == "__main__":
    test_grafana_connection()