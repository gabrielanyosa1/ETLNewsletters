import os.path
import base64
import quopri
import json
import pickle
import logging
import re
import html
import time
from datetime import datetime, timezone, timedelta
from ratelimit import limits, sleep_and_retry
from ratelimit import RateLimitException
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup
from mongo_loader import MongoDBLoader

# Setup logging configuration with DEBUG level
logging.basicConfig(
    level=logging.DEBUG,  # Changed from INFO to DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gmail_filter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define the required access scopes
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Load the FILTER_SENDERS from the JSON file
def load_filter_senders(filename="filter_senders.json"):
    try:
        with open(filename, "r") as f:
            senders = json.load(f)
            if not isinstance(senders, list):
                raise ValueError("Expected a list of email addresses")
            return senders
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON in {filename}: {e}")
    except FileNotFoundError:
        logger.error(f"{filename} not found.")
    except Exception as e:
        logger.error(f"Unexpected error loading filter senders: {e}")
    return []

# Initialize FILTER_SENDERS
FILTER_SENDERS = load_filter_senders()

# Rate limiting constants
CALLS_PER_SECOND = 2  # More conservative limit
CUTOFF_DATE = datetime(2024, 11, 7, tzinfo=timezone.utc)
PAGE_SIZE = 50  # Smaller batch size
BATCH_DELAY = 1  # Delay between batches in seconds

class EmailCleaner:
    """A class to handle email content cleaning and structuring."""
    
    @staticmethod
    def clean_text(text):
        """Clean and normalize text content."""
        if not text:
            return ""
        
        # Extract URLs before cleaning
        urls = EmailCleaner.extract_urls(text)
        
        # Remove URLs from text
        for url in urls:
            text = text.replace(url, '')
            
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Remove special characters and normalize
        text = text.replace('\u200c', '').replace('\ufeff', '')
        
        # Remove any remaining URL artifacts
        text = re.sub(r'http\S*|\[link\]|\[/link\]|\(link\)|\(/link\)', '', text)
        text = re.sub(r'\s+', ' ', text)  # Clean up spaces again
        
        return text.strip()

    @staticmethod
    def extract_urls(text):
        """Extract URLs from text content."""
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        urls = re.findall(url_pattern, text)
        # Remove trailing punctuation from URLs
        cleaned_urls = [re.sub(r'[.,;!?)]+$', '', url) for url in urls]
        return cleaned_urls

    @staticmethod
    def structure_email_body(raw_body):
        """Structure the email body and extract key components."""
        if not raw_body:
            return {
                "clean_text": "",
                "urls": [],
                "length": 0
            }

        # Extract URLs first
        urls = EmailCleaner.extract_urls(raw_body)
        # Then clean text (which will remove URLs)
        cleaned_text = EmailCleaner.clean_text(raw_body)
        
        return {
            "clean_text": cleaned_text,
            "urls": urls,
            "length": len(cleaned_text)
        }

class GmailRateLimiter:
    """Handles rate limiting for Gmail API requests."""
    
    def __init__(self, service):
        self.service = service
        self.backoff_time = 1  # Initial backoff time in seconds
        self.max_retries = 5
        
    def execute_with_backoff(self, func):
        """Execute a function with exponential backoff on rate limit errors."""
        retries = 0
        while retries < self.max_retries:
            try:
                return func()
            except RateLimitException:
                wait_time = self.backoff_time * (2 ** retries)
                logger.warning(f"Rate limit hit, waiting {wait_time} seconds...")
                time.sleep(wait_time)
                retries += 1
            except Exception as e:
                logger.error(f"API error: {e}")
                raise
        raise Exception("Max retries exceeded")
        
    def list_messages(self, query, page_token=None):
        """Rate-limited message listing with backoff."""
        @sleep_and_retry
        @limits(calls=CALLS_PER_SECOND, period=1)
        def _list():
            return self.service.users().messages().list(
                userId='me',
                q=query,
                maxResults=PAGE_SIZE,
                pageToken=page_token
            ).execute()
            
        return self.execute_with_backoff(_list)
            
    @sleep_and_retry
    @limits(calls=CALLS_PER_SECOND, period=1)
    def get_message(self, msg_id):
        """Rate-limited message fetching."""
        try:
            return self.service.users().messages().get(
                userId='me',
                id=msg_id,
                format='full'
            ).execute()
        except Exception as e:
            logger.error(f"Error getting message {msg_id}: {e}")
            raise

def safe_base64_decode(data):
    """Safely decode base64 data, handling padding and non-ASCII characters."""
    try:
        # Add padding if necessary
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
            logger.debug(f"Added {4 - missing_padding} padding characters to base64 data")
            
        # Replace URL-safe characters
        data = data.replace("-", "+").replace("_", "/")
        
        # Handle both string and bytes input
        if isinstance(data, str):
            # Remove any whitespace and newlines
            data = ''.join(data.split())
            
        decoded_data = base64.b64decode(data)
        logger.debug("Successfully decoded base64 data")
        return decoded_data
    except Exception as e:
        logger.error(f"Base64 decoding error: {e}")
        return b''

def decode_content(data, encoding):
    """Decodes email content based on the specified encoding type."""
    try:
        if not data:
            logger.debug("Empty data received for decoding")
            return ""
            
        logger.debug(f"Attempting to decode content with encoding: {encoding}")
        if encoding == 'base64':
            decoded_bytes = safe_base64_decode(data)
            result = decoded_bytes.decode('utf-8', errors='replace')
            logger.debug("Successfully decoded base64 content")
            return result
        elif encoding == 'quoted-printable':
            result = quopri.decodestring(data.encode('utf-8', errors='replace')).decode('utf-8', errors='replace')
            logger.debug("Successfully decoded quoted-printable content")
            return result
        elif encoding == '7bit' or encoding is None:
            logger.debug("No decoding needed for 7bit/None encoding")
            return data
        else:
            logger.warning(f"Unsupported encoding encountered: {encoding}")
            return data
    except Exception as e:
        logger.error(f"Decoding error: {e}")
        return data

def clean_html_content(html_content):
    """Clean and extract text from HTML content."""
    try:
        logger.debug("Starting HTML content cleaning")
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        script_style_count = len(soup(["script", "style"]))
        for element in soup(["script", "style"]):
            element.decompose()
        logger.debug(f"Removed {script_style_count} script/style elements")
            
        # Get text and clean up whitespace
        text = soup.get_text(separator=' ')
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        logger.debug("Successfully cleaned HTML content")
        return text
    except Exception as e:
        logger.error(f"HTML cleaning error: {e}")
        return html_content

def decode_and_extract_text(encoded_body):
    """Decode base64 content and extract readable text."""
    try:
        logger.debug("Starting text extraction from encoded body")
        # First decode from base64
        decoded_bytes = safe_base64_decode(encoded_body)
        decoded_str = decoded_bytes.decode('utf-8', errors='replace')
        
        # Clean and extract text if it's HTML
        if re.search(r'<[^>]+>', decoded_str):
            logger.debug("HTML content detected, cleaning HTML")
            text = clean_html_content(decoded_str)
        else:
            text = decoded_str.strip()
            
        # Structure the cleaned text
        structured_content = EmailCleaner.structure_email_body(text)
        logger.debug("Successfully extracted and structured text content")
        return structured_content
    except Exception as e:
        logger.error(f"Text extraction error: {e}")
        return EmailCleaner.structure_email_body("")

def parse_date(date_str):
    """Parse date string with comprehensive timezone handling."""
    if not date_str:
        raise ValueError("Empty date string")
        
    logger.debug(f"Parsing date string: {date_str!r}")
    
    try:
        # Common timezone mappings
        tz_mappings = {
            'UTC': '+0000',
            'GMT': '+0000',  # Added GMT mapping
            'EST': '-0500',
            'EDT': '-0400',
            'CST': '-0600',
            'CDT': '-0500',
            'PST': '-0800',
            'PDT': '-0700'
        }
        
        # Step 1: Clean up parenthetical timezone info
        main_part = date_str
        parenthetical_tz = None
        if '(' in date_str:
            parts = date_str.split('(')
            main_part = parts[0].strip()
            if len(parts) > 1:
                parenthetical_tz = parts[1].strip(' )')
                logger.debug(f"Found parenthetical timezone: {parenthetical_tz}")
        
        logger.debug(f"Main part after cleaning: {main_part!r}")
        
        # Step 2: Handle single-digit days
        main_part = re.sub(r'(\w{3}), (\d)\b', r'\1, 0\2', main_part)
        logger.debug(f"After padding days: {main_part!r}")
        
        # Step 3: Split into date and timezone parts
        try:
            # Try to parse with timezone directly first
            return datetime.strptime(main_part, "%a, %d %b %Y %H:%M:%S %z")
        except ValueError:
            pass
            
        # Split into date and timezone
        try:
            date_part, tz_part = main_part.rsplit(None, 1)
            logger.debug(f"Split into date ({date_part!r}) and timezone ({tz_part!r})")
        except ValueError as e:
            logger.error(f"Failed to split date and timezone: {e}")
            raise ValueError(f"Invalid date format: {main_part}")
            
        # Step 4: Parse base datetime
        try:
            base_dt = datetime.strptime(date_part, "%a, %d %b %Y %H:%M:%S")
            logger.debug(f"Parsed base datetime: {base_dt}")
        except ValueError as e:
            logger.error(f"Failed to parse datetime part: {e}")
            raise
            
        # Step 5: Handle timezone
        # First check if we have a named timezone
        if tz_part in tz_mappings:
            tz_part = tz_mappings[tz_part]
            logger.debug(f"Mapped named timezone to: {tz_part}")
        elif parenthetical_tz in tz_mappings:
            tz_part = tz_mappings[parenthetical_tz]
            logger.debug(f"Mapped parenthetical timezone to: {tz_part}")
            
        # Ensure timezone starts with + or -
        if not tz_part.startswith(('+', '-')):
            tz_part = f"+{tz_part}"
        
        # Parse timezone offset
        try:
            match = re.match(r'([+-])(\d{2})(\d{2})', tz_part)
            if not match:
                logger.error(f"Invalid timezone format: {tz_part}")
                raise ValueError(f"Invalid timezone format: {tz_part}")
                
            sign, hours, minutes = match.groups()
            logger.debug(f"Timezone components - Sign: {sign}, Hours: {hours}, Minutes: {minutes}")
            
            sign_multiplier = -1 if sign == '-' else 1
            offset = timedelta(
                hours=sign_multiplier * int(hours),
                minutes=sign_multiplier * int(minutes)
            )
            
            # Create timezone-aware datetime
            tz = timezone(offset)
            result = base_dt.replace(tzinfo=tz)
            logger.debug(f"Final datetime: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to process timezone: {str(e)}")
            raise ValueError(f"Failed to process timezone: {str(e)}")
            
    except Exception as e:
        logger.error(f"Date parsing failed for {date_str!r}: {str(e)}")
        raise

def process_message(gmail_limiter, message):
    """Process a single message with improved error handling and cutoff date check."""
    try:
        msg = gmail_limiter.get_message(message['id'])
        payload = msg.get('payload', {})
        headers = payload.get('headers', [])
        
        # Extract headers
        sender = next((h['value'] for h in headers if h['name'] == 'From'), None)
        date = next((h['value'] for h in headers if h['name'] == 'Date'), None)
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), None)
        
        logger.debug(f"Processing message - ID: {message['id']}, Subject: {subject!r}")
        logger.debug(f"Raw date header: {date!r}")
        
        # Parse date with error handling
        msg_date = None
        if date:
            try:
                msg_date = parse_date(date)
                # Early return if message is before cutoff date
                if msg_date < CUTOFF_DATE:
                    logger.debug(f"Message {message['id']} is before cutoff date, skipping")
                    return {'status': 'cutoff', 'date': msg_date}
            except ValueError as e:
                logger.warning(f"Date parsing issue for message {message['id']}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error parsing date: {e}", exc_info=True)
        
        # Filter based on sender
        if not any(email in (sender or '') for email in FILTER_SENDERS):
            logger.debug(f"Sender {sender} not in filter list, skipping")
            return None
            
        logger.debug(f"Matched sender filter: {sender}")
        
        # Process message body
        decoded_body = ""
        parts = payload.get('parts', [])
        
        # Get body content
        if not parts and payload.get('body', {}).get('data'):
            logger.debug("Extracting body from payload directly")
            decoded_body = decode_and_extract_text(payload['body']['data'])
        else:
            logger.debug(f"Processing {len(parts)} message parts")
            for part in parts:
                mime_type = part.get('mimeType')
                if mime_type in ['text/plain', 'text/html']:
                    logger.debug(f"Processing part with MIME type: {mime_type}")
                    body_data = part['body'].get('data', '')
                    if body_data:
                        decoded_body = decode_and_extract_text(body_data)
                        if decoded_body:
                            logger.debug("Successfully extracted body content")
                            break
        
        # Build result dictionary
        result = {
            "id": msg["id"],
            "threadId": msg.get("threadId"),
            "internalDate": msg.get("internalDate"),
            "date": date,
            "parsedDate": msg_date.isoformat() if msg_date else None,
            "from": sender,
            "subject": subject,
            "body": decoded_body,
            "status": "success"
        }
        
        # Only include these fields if they exist
        if "snippet" in msg:
            result["snippet"] = msg["snippet"]
            
        if "labelIds" in msg:
            result["labelIds"] = msg["labelIds"]
            
        return result
        
    except Exception as e:
        logger.error(f"Error processing message {message['id']}: {e}", exc_info=True)
        return None
    
def main():
    """Main function with proper cutoff handling and logging."""
    logger.info(f"Starting Gmail filtering process with cutoff date: {CUTOFF_DATE.isoformat()}")
    
    # Initialize tracking variables and logs
    processed_ids = set()
    filtered_emails = []
    total_processed = 0
    cutoff_reached = False
    consecutive_old_messages = 0
    MAX_OLD_MESSAGES = 5  # Stop after seeing this many consecutive old messages
    
    logs = {
        "start_time": datetime.now().isoformat(),
        "cutoff_date": CUTOFF_DATE.isoformat(),
        "processed_messages": [],
        "stats": {
            "total_processed": 0,
            "successful": 0,
            "skipped": 0,
            "errors": 0
        },
        "errors": []
    }
    
    try:
        # Handle credentials
        creds = None
        if os.path.exists('token.pickle'):
            logger.info("Loading existing credentials from token.pickle")
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
                
        if not creds or not creds.valid:
            logger.info("Obtaining new credentials")
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing expired credentials")
                creds.refresh(Request())
            else:
                logger.info("Initiating OAuth2 flow for new credentials")
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                
            logger.info("Saving new credentials to token.pickle")
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
                
        # Build Gmail API service
        logger.info("Building Gmail API service")
        service = build('gmail', 'v1', credentials=creds)
        gmail_limiter = GmailRateLimiter(service)
        
        # Process messages
        query = 'category:primary OR category:updates'
        page_token = None
        
        while not cutoff_reached:
            try:
                logger.info(f"Fetching next page of messages. Total processed: {total_processed}")
                results = gmail_limiter.list_messages(query, page_token)
                messages = results.get('messages', [])
                
                if not messages:
                    logger.info('No more messages found')
                    break
                    
                for message in messages:
                    try:
                        # Skip duplicates
                        if message['id'] in processed_ids:
                            logger.debug(f"Skipping duplicate message {message['id']}")
                            logs["stats"]["skipped"] += 1
                            continue
                            
                        # Process message
                        processed_message = process_message(gmail_limiter, message)
                        message_log = {
                            "time": datetime.now().isoformat(),
                            "message_id": message['id'],
                            "status": "skipped"
                        }
                        
                        if not processed_message:
                            logs["stats"]["skipped"] += 1
                            logs["processed_messages"].append(message_log)
                            continue
                            
                        # Check if message is before cutoff
                        if processed_message.get('status') == 'cutoff':
                            consecutive_old_messages += 1
                            if consecutive_old_messages >= MAX_OLD_MESSAGES:
                                logger.info(f"Found {MAX_OLD_MESSAGES} consecutive messages before cutoff date. Stopping processing.")
                                cutoff_reached = True
                                break
                            continue
                        else:
                            consecutive_old_messages = 0  # Reset counter when we find a newer message
                        
                        # Add to results if not cutoff
                        filtered_emails.append(processed_message)
                        processed_ids.add(message['id'])
                        total_processed += 1
                        
                        # Update logs
                        message_log.update({
                            "status": "success",
                            "subject": processed_message.get('subject'),
                            "date": processed_message.get('parsedDate')
                        })
                        logs["processed_messages"].append(message_log)
                        logs["stats"]["successful"] += 1
                        
                        logger.info(f"Processed message {total_processed}: {processed_message['subject']}")
                        
                    except Exception as e:
                        error_msg = f"Error processing message {message['id']}: {str(e)}"
                        logger.error(error_msg, exc_info=True)
                        logs["errors"].append({
                            "time": datetime.now().isoformat(),
                            "type": "message_processing_error",
                            "message_id": message['id'],
                            "error": error_msg
                        })
                        logs["stats"]["errors"] += 1
                        continue
                        
                    # Rate limiting delay
                    time.sleep(0.5)
                    
                if cutoff_reached:
                    logger.info("Cutoff date reached. Stopping processing.")
                    break
                    
                # Get next page token
                page_token = results.get('nextPageToken')
                if not page_token:
                    logger.info("No more pages available")
                    break
                    
                # Add delay between pages
                time.sleep(BATCH_DELAY)
                
            except Exception as e:
                error_msg = f"Error processing page: {str(e)}"
                logger.error(error_msg, exc_info=True)
                logs["errors"].append({
                    "time": datetime.now().isoformat(),
                    "type": "page_processing_error",
                    "message": error_msg
                })
                time.sleep(1)
                continue
                
        # Update final stats and save results
        logs["stats"]["total_processed"] = total_processed
        logs["end_time"] = datetime.now().isoformat()
                
        try:
            # Save filtered emails
            logger.info(f"Saving {len(filtered_emails)} filtered emails to JSON")
            with open('filtered_emails.json', 'w', encoding='utf-8') as outfile:
                json.dump(filtered_emails, outfile, indent=4, ensure_ascii=False)
                
            # Save processing logs
            logger.info("Saving processing logs")
            with open('gmail_processing_logs.json', 'w', encoding='utf-8') as logfile:
                json.dump(logs, logfile, indent=4, ensure_ascii=False)
                
            logger.info(f"Successfully saved {len(filtered_emails)} filtered emails and logs")
            logger.info(f"Process completed. Stats: {logs['stats']}")

            # Initializa MongoDB Loader 
            logger.info("Starting MongoDB import process")
            loader = MongoDBLoader()
            if loader.connect():
                mongo_stats = loader.load_emails("filtered_emails.json")
                logger.info(f"MongoDB import completed. Stats: {mongo_stats}")
            else:
                logger.error("Failed to connect to MongoDB")
            loader.close()

        except Exception as e:
            error_msg = f"Failed to save results or load to MongoDb: {str(e)}"
            logs["errors"].append({
                "time": datetime.now().isoformat(),
                "type": "save_error",
                "message": error_msg
            })
            # Try to save logs even if results have failed
            with open('gmail_processing_logs.json', 'w', encoding= 'utf-8') as logfile:
                json.dumps(logs, logfile, indent=4, ensure_ascii= False)
            
        except Exception as e:
            error_msg = f"Failed to save results: {str(e)}"
            logger.error(error_msg, exc_info=True)
            logs["errors"].append({
                "time": datetime.now().isoformat(),
                "type": "save_error",
                "message": error_msg
            })
            # Try to save logs even if results save failed
            with open('gmail_processing_logs.json', 'w', encoding='utf-8') as logfile:
                json.dump(logs, logfile, indent=4, ensure_ascii=False)

    except Exception as e:
        error_msg = f"Fatal error in main: {str(e)}"
        logger.error(error_msg, exc_info=True)
        logs["errors"].append({
            "time": datetime.now().isoformat(),
            "type": "fatal_error",
            "message": error_msg
        })
        with open('gmail_processing_logs.json', 'w', encoding='utf-8') as logfile:
            json.dump(logs, logfile, indent=4, ensure_ascii=False)
        raise
    
if __name__ == '__main__':
    main()