import os
import time
import signal
import smtplib
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, timedelta
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
from gmailextract import main as gmail_main, CUTOFF_DATE
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
        
        # Add persistent count tracking
        self._initial_count = 0
        self._last_known_count = 0
        self._total_processed = 0
        self._final_verification_count = 0  # New tracking variable


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
        """Send email notification with proper formatting."""
        if not all(self.smtp_config.values()):
            logger.debug("Email notification skipped - incomplete configuration")
            return
            
        try:
            message = MIMEMultipart()
            message["From"] = self.smtp_config['user']
            message["To"] = self.smtp_config['recipient']
            message["Subject"] = subject
            
            # Clean up whitespace and indentation, maintain line breaks
            cleaned_lines = []
            for line in body.splitlines():
                if line.strip():  # Only process non-empty lines
                    cleaned_lines.append(line.strip())
                else:
                    cleaned_lines.append('')  # Keep empty lines for spacing
            formatted_body = "\n".join(cleaned_lines)
            
            # Only attach the message once
            message.attach(MIMEText(formatted_body, "plain"))
            
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
                # Initialize database and collection
                if not mongo_loader.initialize_database():
                    raise Exception("Failed to initialize database")
                
                # Store initial count and get collection stats
                self._initial_count = mongo_loader.collection.count_documents({})
                self._last_known_count = self._initial_count
                self._total_processed = 0  # Reset at start
                
                collection_stats = mongo_loader.get_collection_stats()
                
                logger.info(f"Starting processing with {self._initial_count} existing documents")
                if collection_stats and 'date_range' in collection_stats:
                    logger.info(f"Date range: {collection_stats['date_range']['earliest']} to {collection_stats['date_range']['latest']}")
                logger.info(f"Processing emails from {CUTOFF_DATE.isoformat()}")
                
                # Create progress bar with initial count
                with tqdm(desc="Processing emails", unit="email", initial=self._initial_count) as pbar:
                    last_notification_time = datetime.now()
                    NOTIFICATION_INTERVAL = timedelta(minutes=5)
                    
                    try:
                        # Start Gmail extraction
                        gmail_thread = threading.Thread(
                            target=gmail_main,
                            name="GmailExtractor"
                        )
                        gmail_thread.start()
                        
                        while self.running and gmail_thread.is_alive():
                            # Get counts from both MongoDB and JSON
                            current_mongo_count = mongo_loader.collection.count_documents({})

                            try:
                                with open('filtered_emails.json', 'r') as f:
                                    json_data = json.load(f)
                                    json_count = len(json_data)
                            except (FileNotFoundError, json.JSONDecodeError):
                                json_count = 0
                            
                            # Use the larger count and calculate progress
                            current_count = max(current_mongo_count, json_count)
                            if current_count > self._last_known_count:
                                new_processed = current_count - self._last_known_count
                                self._total_processed += new_processed  # Accumulate new processed count
                                
                                # Update progress bar
                                pbar.n = current_count
                                pbar.set_description(f"Processed {self._total_processed} total emails")
                                pbar.refresh()
                                
                                self._last_known_count = current_count
                                self.state.total_processed = self._total_processed
                                
                                logger.info(f"Progress update - Initial: {self._initial_count}, "
                                        f"Current: {current_count}, "
                                        f"New this iteration: {new_processed}, "
                                        f"Total processed: {self._total_processed}")
                                
                                # Check if it's time for a notification
                                current_time = datetime.now()
                                if current_time - last_notification_time >= NOTIFICATION_INTERVAL:
                                    self.send_progress_notification()
                                    last_notification_time = current_time
                                    
                            # Update latest email date
                            latest = mongo_loader.collection.find_one(
                                sort=[("parsedDate", -1)]
                            )
                            if latest and latest.get("parsedDate"):
                                self.state.last_email_date = latest["parsedDate"]
                                logger.debug(f"Latest email timestamp: {self.state.last_email_date}")
                                logger.debug(f"Current time (UTC): {datetime.now(timezone.utc).isoformat()}")
                                time_diff = datetime.now(timezone.utc) - datetime.fromisoformat(self.state.last_email_date.replace('Z', '+00:00'))
                                logger.debug(f"Time difference from latest email: {time_diff}")
                                
                            # Save checkpoint
                            self.save_checkpoint()
                            
                            # Brief pause to reduce CPU usage
                            time.sleep(2)
                            
                        # Wait for Gmail thread to complete
                        gmail_thread.join()
                        
                        # Final verification and counts
                        final_count = mongo_loader.collection.count_documents({})
                        self._final_verification_count = final_count
                        actual_total_processed = final_count - self._initial_count
                        
                        # Ensure counts match
                        if actual_total_processed != self._total_processed:
                            logger.warning(f"Count mismatch detected - Tracked: {self._total_processed}, "
                                        f"Actual: {actual_total_processed}. Using actual count.")
                            self._total_processed = actual_total_processed
                            self.state.total_processed = actual_total_processed
                        
                        # Update final progress bar state
                        pbar.n = final_count
                        pbar.set_description(f"Completed: {self._total_processed} emails processed")
                        pbar.refresh()
                        
                        # Final verification
                        verification_result = self.verify_processing()
                        if verification_result:
                            self.state.status = "completed"
                            logger.info(f"Processing completed successfully. "
                                    f"Initial: {self._initial_count}, "
                                    f"Final: {final_count}, "
                                    f"Total processed: {self._total_processed}")
                            
                            final_stats = {
                                'total_processed': final_count - self._initial_count, 
                                'initial_count': self._initial_count,
                                'final_count': final_count,
                                'last_email_date': self.state.last_email_date,
                                'processing_time': str(datetime.now() - datetime.fromisoformat(self.state.start_time))
                            }

                            success_message = f"""
                            Processing completed successfully!

                            Final Statistics:
                            - Initial collection size: {final_stats['initial_count']}
                            - Final collection size: {final_stats['final_count']}
                            - Total new emails processed: {final_stats['total_processed']}
                            - Latest email date: {final_stats['last_email_date']}
                            - Total processing time: {final_stats['processing_time']}

                            Data is synced and verified between JSON and MongoDB.
                            """

                            self.send_notification(
                                f"Gmail Processing Completed - {final_stats['total_processed']} Emails Processed",
                                success_message
                            )
                        else:
                            logger.warning("Final verification shows discrepancies but data was persisted")
                            self.state.status = "completed_with_warnings"
                            
                            # Send warning notification
                            self.send_notification(
                                "Gmail Processing Completed with Warnings",
                                f"""
                                Processing completed but verification shows some discrepancies.
                                The data has been persisted but may need manual verification.
                                
                                Final State:
                                - Total processed: {self.state.total_processed}
                                - Latest email date: {self.state.last_email_date}
                                - Start time: {self.state.start_time}
                                
                                Please check the logs for more details.
                                """
                            )
                            
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
            # Load MongoDB stats
            mongo_loader = MongoDBLoader()
            if not mongo_loader.connect():
                logger.error("Failed to connect to MongoDB during verification")
                return False
                
            try:
                mongo_loader.initialize_database()
                stats = mongo_loader.get_collection_stats()
                
                if not stats:
                    logger.error("Failed to get MongoDB stats")
                    return False
                    
                # Verify data consistency using verify_state
                try:
                    with open('filtered_emails.json', 'r') as f:
                        json_data = json.load(f)
                    
                    # Compare document counts
                    json_count = len(json_data)
                    mongo_count = stats['total_documents']
                    
                    if json_count != mongo_count:
                        logger.error(f"Document count mismatch: JSON={json_count}, MongoDB={mongo_count}")
                        return False
                        
                    # Compare IDs
                    json_ids = set(email['id'] for email in json_data)
                    mongo_ids = set(str(doc['id']) for doc in mongo_loader.collection.find({}, {'id': 1}))
                    
                    if json_ids != mongo_ids:
                        logger.error("ID sets don't match between JSON and MongoDB")
                        return False
                        
                    # Check date ranges
                    if 'date_range' in stats:
                        logger.info(f"Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
                    
                    # All checks passed
                    logger.info("Verification successful: JSON and MongoDB are in sync")
                    return True
                    
                except FileNotFoundError:
                    logger.error("filtered_emails.json not found")
                    return False
                except Exception as e:
                    logger.error(f"Error during verification: {e}")
                    return False
                    
            finally:
                mongo_loader.close()
                
        except Exception as e:
            logger.error(f"Verification error: {e}")
            return False
            
    def save_checkpoint(self):
        """Save processing checkpoint with accurate counts."""
        try:
            checkpoint_data = {
                'initial_count': self._initial_count,
                'last_known_count': self._last_known_count,
                'total_processed': self._total_processed,
                'final_verification_count': self._final_verification_count,
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
        """Load processing checkpoint with count reset."""
        try:
            if self.checkpoint_path.exists():
                with open(self.checkpoint_path, 'r') as f:
                    checkpoint_data = json.load(f)
                    
                # Only load status and time-related fields
                self.state.last_email_date = checkpoint_data.get('last_email_date')
                self.state.last_backup_time = checkpoint_data.get('last_backup_time')
                self.state.start_time = checkpoint_data.get('start_time')
                self.state.status = checkpoint_data.get('status', 'resumed')
                
                # Reset counters for new run
                self._initial_count = 0
                self._last_known_count = 0
                self._total_processed = 0
                self._final_verification_count = 0
                self.state.total_processed = 0
                
                logger.info("Checkpoint loaded - counters reset for new run")
                return True
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
        return False
        
    def send_progress_notification(self):
        """Send progress update notification with accurate counts."""
        try:
            # Get current MongoDB count
            mongo_loader = MongoDBLoader()
            if mongo_loader.connect():
                current_count = mongo_loader.collection.count_documents({})
                mongo_loader.close()
            else:
                current_count = self._last_known_count
                
            processing_time = datetime.now() - datetime.fromisoformat(self.state.start_time)
            processing_time_str = str(processing_time).split('.')[0]
            
            subject = f"Gmail Processing Update: {self._total_processed} Emails Processed"
            body = f"""
            Processing Status Update:
            - Initial collection size: {self._initial_count}
            - Current collection size: {current_count}
            - New emails processed: {self._total_processed}
            - Latest email date: {self.state.last_email_date}
            - Processing time: {processing_time_str}
            - Current status: {self.state.status}
            - Last backup: {self.state.last_backup_time}
            
            Processing is ongoing...
            """
            
            logger.info(f"Sending progress notification - Processed: {self._total_processed}")
            self.send_notification(subject, body)
            
        except Exception as e:
            logger.error(f"Error sending progress notification: {e}")
        
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
        """Cleanup resources and reset checkpoint."""
        if hasattr(self, 'power_handler'):
            self.power_handler.allow_sleep()
        if self.running:
            self.running = False
            self.create_backup(force=True)
            
            # Reset or remove checkpoint
            try:
                if self.checkpoint_path.exists():
                    os.remove(self.checkpoint_path)
                    logger.info("Checkpoint file removed during cleanup")
            except Exception as e:
                logger.error(f"Error removing checkpoint file: {e}")
                
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