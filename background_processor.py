import os
import time
import signal
import smtplib
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from tqdm import tqdm
import logging
import json
from typing import Optional
import threading
from dataclasses import dataclass
from pathlib import Path
import atexit
import platform
from contextlib import contextmanager
from dotenv import load_dotenv

# Import your existing modules
from gmailextract import main as gmail_main, CUTOFF_DATE  # Note the filename correction
from verify_state import verify_state
from sync_mongodb import sync_mongodb
from mongo_loader import MongoDBLoader

# Load environment variables
load_dotenv()

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] %(message)s',
    handlers=[
        logging.FileHandler('background_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingState:
    """Track the current state of processing."""
    total_processed: int = 0
    current_batch: int = 0
    last_email_date: Optional[str] = None
    last_backup_time: Optional[str] = None
    start_time: Optional[str] = None
    status: str = "initializing"

class BackgroundProcessor:
    """Handle background processing of Gmail data."""
    NOTIFICATION_THRESHOLD = 1000  # emails
    BACKUP_INTERVAL = 1800  # seconds (30 minutes)
    
    def __init__(self):
        self.state = ProcessingState()
        self.power_handler = MacOSPowerAssertionHandler()
        self.running = False
        self.checkpoint_path = Path('processing_checkpoint.json')
        self.backup_dir = Path('backups')
        self.backup_dir.mkdir(exist_ok=True)
        
        # Email configuration from .env
        self.smtp_config = {
            'server': 'smtp.gmail.com',
            'port': 465,
            'user': os.getenv('GMAIL_USER'),
            'password': os.getenv('GMAIL_APP_PASSWORD'),
            'recipient': os.getenv('NOTIFICATION_EMAIL')
        }
        
        # Validate email configuration
        if not all(self.smtp_config.values()):
            logger.warning("Email configuration incomplete. Notifications will be disabled.")
            
    def send_notification(self, subject: str, body: str):
        """Send email notification with error handling."""
        if not all(self.smtp_config.values()):
            logger.debug("Email notification skipped - incomplete configuration")
            return
            
        try:
            message = MIMEMultipart()
            message["From"] = self.smtp_config['user']
            message["To"] = self.smtp_config['recipient']
            message["Subject"] = subject
            message.attach(MIMEText(body, "plain"))
            
            with smtplib.SMTP_SSL(self.smtp_config['server'], self.smtp_config['port']) as server:
                server.login(self.smtp_config['user'], self.smtp_config['password'])
                server.send_message(message)
            logger.info(f"Notification sent: {subject}")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
            
    def create_backup(self, force: bool = False):
        """Create backup with error handling."""
        try:
            current_time = datetime.now()
            
            # Check if enough time has passed since last backup
            if not force and self.state.last_backup_time:
                last_backup = datetime.fromisoformat(self.state.last_backup_time)
                if (current_time - last_backup).total_seconds() < 3600:  # 1 hour
                    return
                    
            timestamp = current_time.strftime('%Y%m%d_%H%M%S')
            
            # Backup filtered emails
            if os.path.exists('filtered_emails.json'):
                backup_path = self.backup_dir / f"filtered_emails_{timestamp}.json"
                with open('filtered_emails.json', 'r') as source:
                    with open(backup_path, 'w') as target:
                        json.dump(json.load(source), target, indent=4)
                        
            # Backup checkpoint
            if os.path.exists(self.checkpoint_path):
                backup_path = self.backup_dir / f"checkpoint_{timestamp}.json"
                with open(self.checkpoint_path, 'r') as source:
                    with open(backup_path, 'w') as target:
                        json.dump(json.load(source), target, indent=4)
                        
            self.state.last_backup_time = current_time.isoformat()
            logger.info(f"Backup created at {timestamp}")
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            
    def process_emails(self):
        """Main email processing function with progress tracking."""
        try:
            self.state.start_time = datetime.now().isoformat()
            self.state.status = "processing"
            
            # Initialize MongoDB connection
            mongo_loader = MongoDBLoader()
            if not mongo_loader.connect():
                raise Exception("Failed to connect to MongoDB")
                
            try:
                # Get initial count
                initial_count = mongo_loader.collection.count_documents({})
                logger.info(f"Starting processing with {initial_count} existing documents")
                logger.info(f"Processing emails from {CUTOFF_DATE.isoformat()}")
                
                # Create progress bar
                with tqdm(desc="Processing emails", unit="email") as pbar:
                    last_count = initial_count
                    
                    try:
                        # Start Gmail extraction
                        gmail_thread = threading.Thread(
                            target=gmail_main,
                            name="GmailExtractor"
                        )
                        gmail_thread.start()
                        
                        while self.running and gmail_thread.is_alive():
                            current_count = mongo_loader.collection.count_documents({})
                            new_processed = current_count - last_count
                            
                            if new_processed > 0:
                                pbar.update(new_processed)
                                last_count = current_count
                                self.state.total_processed = current_count - initial_count
                                
                                # Check for notification/backup threshold
                                if self.state.total_processed % 1000 == 0:
                                    self.send_progress_notification()
                                    self.create_backup()
                                    
                            # Update latest email date
                            latest = mongo_loader.collection.find_one(
                                sort=[("parsedDate", -1)]
                            )
                            if latest and latest.get("parsedDate"):
                                self.state.last_email_date = latest["parsedDate"]
                                
                            # Save checkpoint
                            self.save_checkpoint()
                            
                            # Brief pause to reduce CPU usage
                            time.sleep(5)
                            
                        # Wait for Gmail thread to complete
                        gmail_thread.join()
                        
                        # Final verification
                        if self.verify_processing():
                            self.state.status = "completed"
                            logger.info("Processing completed successfully")
                        else:
                            raise Exception("Final verification failed")
                            
                    except Exception as e:
                        self.state.status = "error"
                        logger.error(f"Gmail processing error: {e}", exc_info=True)
                        raise
                        
            finally:
                mongo_loader.close()
                
        except Exception as e:
            self.state.status = "error"
            logger.error(f"Processing error: {e}", exc_info=True)
            self.send_error_notification(str(e))
            raise
            
    def verify_processing(self) -> bool:
        """Verify processing status and completion."""
        try:
            # Verify data consistency
            if verify_state() and sync_mongodb():
                logger.info("Data verification successful")
                return True
            return False
        except Exception as e:
            logger.error(f"Verification error: {e}")
            return False
            
    def save_checkpoint(self):
        """Save processing checkpoint."""
        try:
            checkpoint_data = {
                'total_processed': self.state.total_processed,
                'current_batch': self.state.current_batch,
                'last_email_date': self.state.last_email_date,
                'last_backup_time': self.state.last_backup_time,
                'start_time': self.state.start_time,
                'status': self.state.status,
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.checkpoint_path, 'w') as f:
                json.dump(checkpoint_data, f, indent=4)
                
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
            
    def load_checkpoint(self) -> bool:
        """Load processing checkpoint if it exists."""
        try:
            if self.checkpoint_path.exists():
                with open(self.checkpoint_path, 'r') as f:
                    checkpoint_data = json.load(f)
                    
                self.state.total_processed = checkpoint_data.get('total_processed', 0)
                self.state.current_batch = checkpoint_data.get('current_batch', 0)
                self.state.last_email_date = checkpoint_data.get('last_email_date')
                self.state.last_backup_time = checkpoint_data.get('last_backup_time')
                self.state.start_time = checkpoint_data.get('start_time')
                self.state.status = checkpoint_data.get('status', 'resumed')
                
                logger.info(f"Resumed from checkpoint: {self.state.total_processed} items processed")
                return True
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
        return False
        
    def send_progress_notification(self):
        """Send progress update notification."""
        subject = f"Gmail Processing Update: {self.state.total_processed} emails processed"
        body = f"""
        Processing Status Update:
        - Total emails processed: {self.state.total_processed}
        - Latest email date: {self.state.last_email_date}
        - Processing started: {self.state.start_time}
        - Current status: {self.state.status}
        - Last backup: {self.state.last_backup_time}
        """
        self.send_notification(subject, body)
        
    def send_error_notification(self, error_message: str):
        """Send error notification."""
        subject = "Gmail Processing Error"
        body = f"""
        Error in Gmail processing:
        {error_message}
        
        Processing Details:
        - Start time: {self.state.start_time}
        - Processed before error: {self.state.total_processed}
        - Last successful email date: {self.state.last_email_date}
        """
        self.send_notification(subject, body)
        
    def start(self):
        """Start background processing."""
        try:
            logger.info("Starting background processor...")
            self.running = True
            
            # Register cleanup handlers
            atexit.register(self.cleanup)
            signal.signal(signal.SIGINT, self.handle_shutdown)
            signal.signal(signal.SIGTERM, self.handle_shutdown)
            
            # Prevent system sleep
            self.power_handler.prevent_sleep()
            
            # Load checkpoint if exists
            self.load_checkpoint()
            
            # Start processing in a separate thread
            processing_thread = threading.Thread(
                target=self.process_emails,
                name="EmailProcessor"
            )
            processing_thread.start()
            
            # Wait for processing to complete
            processing_thread.join()
            
        except Exception as e:
            logger.error(f"Error in background processing: {e}", exc_info=True)
            self.send_error_notification(str(e))
            raise
        finally:
            self.cleanup()
            
    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received. Cleaning up...")
        self.running = False
        
    def cleanup(self):
        """Cleanup resources."""
        if hasattr(self, 'power_handler'):
            self.power_handler.allow_sleep()
        if self.running:
            self.running = False
            self.create_backup(force=True)
            logger.info("Cleanup completed")

class MacOSPowerAssertionHandler:
    """Prevent system sleep on macOS."""
    
    def __init__(self):
        self.caffeinate_process = None
        self.caffeinate_path = '/usr/bin/caffeinate'  # Add explicit path
        
    def prevent_sleep(self):
        """Prevent system sleep using native macOS caffeinate."""
        if platform.system() == 'Darwin':  # macOS
            try:
                if os.path.exists(self.caffeinate_path):
                    self.caffeinate_process = subprocess.Popen(
                        [self.caffeinate_path, '-i', '-m'],  # -i: prevent idle sleep, -m: prevent disk sleep
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    logger.info("Sleep prevention enabled using caffeinate")
                else:
                    # Fallback to pmset if caffeinate isn't found (unlikely on macOS)
                    subprocess.run(['/usr/bin/pmset', 'noidle'], 
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
                    logger.info("Sleep prevention enabled using pmset")
            except Exception as e:
                logger.warning(f"Could not prevent sleep: {e}. Processing will continue but system may sleep.")
                
    def allow_sleep(self):
        """Allow system sleep."""
        if self.caffeinate_process:
            try:
                self.caffeinate_process.terminate()
                self.caffeinate_process.wait()
                logger.info("Sleep prevention disabled")
            except Exception as e:
                logger.error(f"Error disabling sleep prevention: {e}")

def main():
    """Main entry point for background processing."""
    try:
        # Start the background processor
        processor = BackgroundProcessor()
        processor.start()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()