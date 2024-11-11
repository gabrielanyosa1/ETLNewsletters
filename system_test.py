import os
import time
import logging
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from mongo_loader import MongoDBLoader
import subprocess
import platform

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SystemTest:
    """Test all components of the background processing system."""
    
    def __init__(self):
        load_dotenv()
        self.tests_passed = 0
        self.tests_failed = 0
        
    def run_all_tests(self):
        """Run all system tests."""
        logger.info("Starting system tests...")
        
        tests = [
            self.test_env_variables,
            self.test_mongodb_connection,
            self.test_email_notification,
            self.test_sleep_prevention,
            self.test_backup_directory
        ]
        
        for test in tests:
            try:
                test()
                self.tests_passed += 1
            except Exception as e:
                logger.error(f"Test failed: {test.__name__} - {str(e)}")
                self.tests_failed += 1
                
        self.print_results()
        
    def test_env_variables(self):
        """Test environment variables are properly set."""
        logger.info("Testing environment variables...")
        required_vars = [
            'MONGODB_URI',
            'GMAIL_USER',
            'GMAIL_APP_PASSWORD',
            'NOTIFICATION_EMAIL'
        ]
        
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")
            
        logger.info("✓ Environment variables test passed")
        
    def test_mongodb_connection(self):
        """Test MongoDB connection."""
        logger.info("Testing MongoDB connection...")
        loader = MongoDBLoader()
        
        if not loader.connect():
            raise ConnectionError("Failed to connect to MongoDB")
            
        try:
            # Test basic operations
            loader.initialize_database()
            stats = loader.get_collection_stats()
            if stats:
                logger.info(f"✓ MongoDB connection test passed (Documents: {stats['total_documents']})")
        finally:
            loader.close()
            
    def test_email_notification(self):
        """Test email notification system."""
        logger.info("Testing email notification system...")
        
        try:
            message = MIMEMultipart()
            message["From"] = os.getenv("GMAIL_USER")
            message["To"] = os.getenv("NOTIFICATION_EMAIL")
            message["Subject"] = "Gmail Processing System Test"
            
            body = """
            This is a test email from your Gmail Processing System.
            If you receive this, your notification system is working correctly.
            
            Current time: {}
            """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            message.attach(MIMEText(body, "plain"))
            
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(
                    os.getenv("GMAIL_USER"),
                    os.getenv("GMAIL_APP_PASSWORD")
                )
                server.send_message(message)
                
            logger.info("✓ Email notification test passed")
            
        except Exception as e:
            raise Exception(f"Email notification test failed: {str(e)}")
            
    def test_sleep_prevention(self):
        """Test sleep prevention mechanism."""
        logger.info("Testing sleep prevention...")
        
        if platform.system() == 'Darwin':  # macOS
            caffeinate_path = '/usr/bin/caffeinate'
            try:
                # Test if caffeinate exists and is executable
                if not os.path.exists(caffeinate_path):
                    raise FileNotFoundError(f"Caffeinate not found at {caffeinate_path}")
                    
                # Test if we can run it briefly
                result = subprocess.run(
                    [caffeinate_path, '-i', '-t', '1'],  # Run for 1 second
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=2  # Wait max 2 seconds
                )
                
                if result.returncode == 0:
                    logger.info("✓ Sleep prevention test passed")
                else:
                    raise RuntimeError(f"Caffeinate test failed with return code {result.returncode}")
            except Exception as e:
                raise RuntimeError(f"Sleep prevention test failed: {str(e)}")
        else:
            logger.warning("Sleep prevention test skipped (not on macOS)")
            
    def test_backup_directory(self):
        """Test backup directory creation and permissions."""
        logger.info("Testing backup system...")
        
        backup_dir = os.path.join(os.getcwd(), 'backups')
        test_file = os.path.join(backup_dir, 'test.txt')
        
        # Create backup directory if it doesn't exist
        os.makedirs(backup_dir, exist_ok=True)
        
        # Test write permissions
        try:
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            logger.info("✓ Backup system test passed")
        except Exception as e:
            raise Exception(f"Backup system test failed: {str(e)}")
            
    def print_results(self):
        """Print test results."""
        logger.info("\n=== Test Results ===")
        logger.info(f"Tests passed: {self.tests_passed}")
        logger.info(f"Tests failed: {self.tests_failed}")
        logger.info("==================\n")
        
        if self.tests_failed == 0:
            logger.info("All systems are ready for processing!")
        else:
            logger.warning("Some tests failed. Please check the logs above.")

def main():
    """Run system tests."""
    tester = SystemTest()
    tester.run_all_tests()

if __name__ == "__main__":
    main()