import os.path
import base64
import quopri
import json
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup
import re

# Define the required access scopes
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# List of sender email addresses to filter
FILTER_SENDERS = [
    'noreply@news.bloomberg.com',
    'publishing@email.mckinsey.com',
    'account@seekingalpha.com',
    'subscriptions@seekingalpha.com',
    'newsletters-noreply@linkedin.com',
    'members@medium.com',
    'noreply@medium.com',
    'news@alphasignal.ai',
    'squad@thedailyupside.com',
    'therundownai@mail.beehiiv.com',
    'bayareatimes@bayareatimes.com',
    'hello@mindstream.news'
]

def safe_base64_decode(data):
    """Safely decode base64 data, handling padding and non-ASCII characters."""
    try:
        # Add padding if necessary
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
            
        # Replace URL-safe characters
        data = data.replace("-", "+").replace("_", "/")
        
        # Handle both string and bytes input
        if isinstance(data, str):
            # Remove any whitespace and newlines
            data = ''.join(data.split())
            
        return base64.b64decode(data)
    except Exception as e:
        print(f"Base64 decoding error: {e}")
        return b''

def decode_content(data, encoding):
    """Decodes email content based on the specified encoding type."""
    try:
        if not data:
            return ""
            
        if encoding == 'base64':
            decoded_bytes = safe_base64_decode(data)
            return decoded_bytes.decode('utf-8', errors='replace')
        elif encoding == 'quoted-printable':
            return quopri.decodestring(data.encode('utf-8', errors='replace')).decode('utf-8', errors='replace')
        elif encoding == '7bit' or encoding is None:
            return data
        else:
            print(f"Unsupported encoding: {encoding}")
            return data
    except Exception as e:
        print(f"Decoding error: {e}")
        return data

def clean_html_content(html_content):
    """Clean and extract text from HTML content."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for element in soup(["script", "style"]):
            element.decompose()
            
        # Get text and clean up whitespace
        text = soup.get_text(separator=' ')
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    except Exception as e:
        print(f"HTML cleaning error: {e}")
        return html_content

def decode_and_extract_text(encoded_body):
    """Decode base64 content and extract readable text."""
    try:
        # First decode from base64
        decoded_bytes = safe_base64_decode(encoded_body)
        decoded_str = decoded_bytes.decode('utf-8', errors='replace')
        
        # Clean and extract text if it's HTML
        if re.search(r'<[^>]+>', decoded_str):
            return clean_html_content(decoded_str)
        
        return decoded_str.strip()
    except Exception as e:
        print(f"Text extraction error: {e}")
        return ""

def main():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    try:
        service = build('gmail', 'v1', credentials=creds)
        query = 'category:primary OR category:updates'
        results = service.users().messages().list(userId='me', q=query, maxResults=100).execute()
        messages = results.get('messages', [])

        if not messages:
            print('No messages found.')
            return

        filtered_emails = []
        for message in messages:
            try:
                msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
                payload = msg.get('payload', {})
                headers = payload.get('headers', [])
                
                # Extract headers
                sender = next((h['value'] for h in headers if h['name'] == 'From'), None)
                date = next((h['value'] for h in headers if h['name'] == 'Date'), None)
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), None)

                # Filter based on sender
                if sender and any(email in sender for email in FILTER_SENDERS):
                    decoded_body = ""
                    parts = payload.get('parts', [])
                    
                    # If no parts, try to get body from payload directly
                    if not parts and payload.get('body', {}).get('data'):
                        decoded_body = decode_and_extract_text(payload['body']['data'])
                    else:
                        for part in parts:
                            mime_type = part.get('mimeType')
                            if mime_type in ['text/plain', 'text/html']:
                                body_data = part['body'].get('data', '')
                                if body_data:
                                    decoded_body = decode_and_extract_text(body_data)
                                    if decoded_body:
                                        break

                    filtered_emails.append({
                        "id": msg["id"],
                        "threadId": msg["threadId"],
                        "labelIds": msg["labelIds"],
                        "snippet": msg["snippet"],
                        "internalDate": msg["internalDate"],
                        "date": date,
                        "from": sender,
                        "subject": subject,
                        "body": decoded_body
                    })
            except Exception as e:
                print(f"Error processing message {message['id']}: {e}")
                continue

        # Save the filtered emails to a JSON file
        with open('filtered_emails.json', 'w', encoding='utf-8') as outfile:
            json.dump(filtered_emails, outfile, indent=4, ensure_ascii=False)

        print(f'Saved {len(filtered_emails)} emails to filtered_emails.json')

    except HttpError as error:
        print(f'An error occurred: {error}')

if __name__ == '__main__':
    main()