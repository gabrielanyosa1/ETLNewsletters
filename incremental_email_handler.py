import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Set, Optional
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class EmailMetadata:
    """Store essential email metadata for comparison."""
    id: str
    date: str
    from_address: str
    subject: str

    @classmethod
    def from_email_dict(cls, email_dict: Dict) -> 'EmailMetadata':
        return cls(
            id=email_dict['id'],
            date=email_dict.get('parsedDate', ''),
            from_address=email_dict.get('from', ''),
            subject=email_dict.get('subject', '')
        )

class IncrementalEmailHandler:
    """Handles incremental updates to email collection."""
    
    def __init__(self, json_path: str = 'filtered_emails.json'):
        """
        Initialize the handler.
        
        Args:
            json_path: Path to the JSON file storing filtered emails
        """
        self.json_path = json_path
        self.existing_emails: List[Dict] = []
        self.existing_ids: Set[str] = set()
        self.load_existing_data()
        
    def load_existing_data(self) -> None:
        """Load existing email data from JSON file."""
        try:
            if os.path.exists(self.json_path):
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    self.existing_emails = json.load(f)
                    self.existing_ids = {email['id'] for email in self.existing_emails}
                logger.info(f"Loaded {len(self.existing_emails)} existing emails")
            else:
                logger.info("No existing email file found. Starting fresh.")
        except Exception as e:
            logger.error(f"Error loading existing emails: {e}")
            raise

    def backup_existing_file(self) -> None:
        """Create a backup of the existing JSON file."""
        if os.path.exists(self.json_path):
            backup_dir = Path('backups')
            backup_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = backup_dir / f"filtered_emails_{timestamp}.json"
            
            try:
                with open(self.json_path, 'r', encoding='utf-8') as source:
                    with open(backup_path, 'w', encoding='utf-8') as target:
                        json.dump(json.load(source), target, indent=4)
                logger.info(f"Created backup at {backup_path}")
            except Exception as e:
                logger.error(f"Error creating backup: {e}")
                raise

    def merge_emails(self, new_emails: List[Dict]) -> List[Dict]:
        """
        Merge new emails with existing ones, avoiding duplicates.
        
        Args:
            new_emails: List of new email dictionaries to merge
            
        Returns:
            List of merged emails
        """
        merged = self.existing_emails.copy()
        new_count = 0
        duplicate_count = 0
        
        # Create lookup for faster comparison
        existing_lookup = {
            email['id']: EmailMetadata.from_email_dict(email)
            for email in self.existing_emails
        }
        
        for email in new_emails:
            if email['id'] not in existing_lookup:
                merged.append(email)
                new_count += 1
            else:
                duplicate_count += 1
                
        logger.info(f"Merged {new_count} new emails (skipped {duplicate_count} duplicates)")
        return merged

    def save_merged_emails(self, merged_emails: List[Dict]) -> None:
        """
        Save merged emails to JSON file.
        
        Args:
            merged_emails: List of all emails to save
        """
        try:
            # Create backup before saving
            self.backup_existing_file()
            
            # Sort emails by date before saving
            sorted_emails = sorted(
                merged_emails,
                key=lambda x: x.get('parsedDate', ''),
                reverse=True  # Most recent first
            )
            
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(sorted_emails, f, indent=4, ensure_ascii=False)
            logger.info(f"Saved {len(sorted_emails)} emails to {self.json_path}")
            
        except Exception as e:
            logger.error(f"Error saving merged emails: {e}")
            raise

    def process_new_emails(self, new_emails: List[Dict]) -> List[Dict]:
        """
        Process new emails and merge with existing ones.
        
        Args:
            new_emails: List of new email dictionaries
            
        Returns:
            List of merged emails
        """
        try:
            logger.info(f"Processing {len(new_emails)} new emails")
            
            # Merge emails
            merged_emails = self.merge_emails(new_emails)
            
            # Save merged result
            self.save_merged_emails(merged_emails)
            
            return merged_emails
            
        except Exception as e:
            logger.error(f"Error processing new emails: {e}")
            raise

    def get_latest_email_date(self) -> Optional[str]:
        """
        Get the date of the most recent email in the existing data.
        
        Returns:
            ISO format date string or None if no existing emails
        """
        if not self.existing_emails:
            return None
            
        dates = [
            email.get('parsedDate') 
            for email in self.existing_emails 
            if email.get('parsedDate')
        ]
        
        return max(dates) if dates else None

    def get_statistics(self) -> Dict:
        """
        Get statistics about the email collection.
        
        Returns:
            Dictionary with collection statistics
        """
        if not self.existing_emails:
            return {
                "total_emails": 0,
                "date_range": None,
                "senders": {}
            }
            
        dates = [
            email.get('parsedDate') 
            for email in self.existing_emails 
            if email.get('parsedDate')
        ]
        
        senders = {}
        for email in self.existing_emails:
            sender = email.get('from', 'Unknown')
            senders[sender] = senders.get(sender, 0) + 1
            
        return {
            "total_emails": len(self.existing_emails),
            "date_range": {
                "earliest": min(dates) if dates else None,
                "latest": max(dates) if dates else None
            },
            "senders": dict(sorted(
                senders.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:5])
        }

def main():
    """Example usage of IncrementalEmailHandler."""
    handler = IncrementalEmailHandler()
    
    # Get current statistics
    stats = handler.get_statistics()
    logger.info("Current collection statistics:")
    logger.info(f"Total emails: {stats['total_emails']}")
    if stats['date_range']:
        logger.info(f"Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
    logger.info("Top senders:")
    for sender, count in stats['senders'].items():
        logger.info(f"  {sender}: {count} emails")

if __name__ == "__main__":
    main()