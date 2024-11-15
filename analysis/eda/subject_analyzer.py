import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Set
from dateutil import parser
from collections import defaultdict

class EmailSubjectAnalyzer:
    def __init__(self, mongo_loader):
        """Initialize with existing MongoDB loader."""
        self.mongo_loader = mongo_loader
        
        # Initialize database and collection
        self.mongo_loader.initialize_database()
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize categories with sorted keywords for consistency
        self.categories = {
            'crypto': sorted(['bitcoin', 'crypto', 'blockchain', 'defi', 'nft', 'eth']),
            'tech': sorted(['ai', 'tech', 'software', 'cloud', 'saas']),
            'finance': sorted(['market', 'stock', 'invest', 'trading']),
            'macro': sorted(['economy', 'inflation', 'gdp', 'fed'])
        }
        
        # Sort stop words for consistency
        self.stop_words = sorted({
            'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i',
            'it', 'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at',
            'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her',
            'she', 'or', 'an', 'will', 'my', 'one', 'all', 'would', 'there',
            'their', 'what', 'so', 'up', 'out', 'if', 'about', 'who', 'get',
            'which', 'go', 'me', 'when', 'make', 'can', 'like', 'time', 'no',
            'just', 'him', 'know', 'take', 'people', 'into', 'year', 'your',
            'good', 'some', 'could', 'them', 'see', 'other', 'than', 'then',
            'now', 'look', 'only', 'come', 'its', 'over', 'think', 'also',
            'back', 'after', 'use', 'two', 'how', 'our', 'work', 'first',
            'well', 'way', 'even', 'new', 'want', 'because', 'any', 'these',
            'give', 'day', 'most', 'us', 'more', 'has', 'was', 'are', 'been',
            'had', 'were', 'is', 'did', 'does', 'why', 'where', 'who', 'which',
            'when', 'what', 'how', 'many', 'much', 'very', 'should', 'would',
            'could', 'might', 'must', 'shall', 'will', 'may', 'can', 'data'
        })

    def extract_meaningful_themes(self, text: str) -> List[str]:
        """Extract meaningful themes deterministically."""
        # Split and sort words for consistency
        words = sorted(text.lower().split())
        
        # Filter deterministically
        meaningful_words = []
        for word in words:
            if (word not in self.stop_words and 
                len(word) > 3 and 
                not word.isdigit() and 
                not any(cat in word for cat in sorted(self.categories.keys()))):
                meaningful_words.append(word)
        
        return meaningful_words

    def _categorize_subject(self, subject: str) -> Set[str]:
        """Categorize subject deterministically."""
        found_categories = set()
        # Process categories in sorted order
        for category, keywords in sorted(self.categories.items()):
            if any(keyword in subject for keyword in keywords):
                found_categories.add(category)
        return found_categories

    def analyze_subjects(self) -> Dict:
        """Analyze email subjects from MongoDB."""
        try:
            # Get all emails from collection
            cursor = self.mongo_loader.collection.find({}, {"subject": 1, "parsedDate": 1})

            processed_data = {
                'time_series': [],
                'category_counts': {cat: 0 for cat in self.categories.keys()},
                'themes': []
            }

            for doc in cursor:
                subject = doc.get('subject', '').lower()
                date_str = doc.get('parsedDate')
                
                if not subject or not date_str:
                    continue

                # Convert ISO string to Unix timestamp immediately
                try:
                    dt = parser.parse(date_str)
                    timestamp_ms = int(dt.timestamp() * 1000)  # Convert to Unix milliseconds
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Error parsing date {date_str}: {e}")
                    continue

                # Category analysis
                for category, keywords in self.categories.items():
                    if any(keyword in subject for keyword in keywords):
                        processed_data['category_counts'][category] += 1
                        processed_data['time_series'].append({
                            'timestamp': timestamp_ms,  # Store as Unix timestamp
                            'category': category,
                            'value': 1
                        })

                # Extract meaningful themes
                themes = self.extract_meaningful_themes(subject)
                processed_data['themes'].extend(themes)

            self.logger.info(f"Processed {len(processed_data['time_series'])} data points")
            return processed_data

        except Exception as e:
            self.logger.error(f"Error in analyze_subjects: {str(e)}")
            raise

    def _validate_formatted_data(self, data: Dict) -> None:
        """Validate the formatted data before returning."""
        try:
            # Check required keys
            required_keys = ['categories', 'time_series', 'category_totals', 'top_themes']
            missing_keys = [key for key in required_keys if key not in data]
            if missing_keys:
                raise ValueError(f"Missing required keys: {missing_keys}")

            # Validate time series timestamps
            if data['time_series']:
                invalid_timestamps = [
                    entry for entry in data['time_series']
                    if not isinstance(entry.get('timestamp'), (int, float))
                ]
                if invalid_timestamps:
                    raise ValueError(f"Invalid timestamps found: {invalid_timestamps[:3]}")

            # Validate categories consistency
            categories_set = set(data['categories'])
            time_series_categories = {entry['category'] for entry in data['time_series']}
            total_categories = set(data['category_totals'].keys())

            if not time_series_categories.issubset(categories_set):
                raise ValueError("Time series contains unknown categories")
            if not total_categories.issubset(categories_set):
                raise ValueError("Category totals contains unknown categories")

        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            raise ValueError(f"Data validation failed: {str(e)}")

    def run_analysis(self) -> Dict:
        """Run the complete analysis pipeline with deterministic processing."""
        try:
            self.logger.info("Starting subject analysis...")
            analysis_results = self.analyze_subjects()

            # Add debug logging for initial data
            if analysis_results.get('time_series'):
                sample_data = analysis_results['time_series'][:2]
                self.logger.debug(f"Sample raw time series data: {sample_data}")

            # Process time series data deterministically
            if not analysis_results.get('time_series'):
                self.logger.warning("No time series data found in analysis results")
                time_series = []
            else:
                df = pd.DataFrame(analysis_results['time_series'])
                
                # Debug logging
                self.logger.debug(f"DataFrame columns: {df.columns}")
                self.logger.debug(f"Sample raw timestamp values: {df['timestamp'].head()}")
                
                try:
                    # Validate timestamps are numeric
                    numeric_timestamps = pd.to_numeric(df['timestamp'], errors='coerce')
                    if not numeric_timestamps.notnull().all():
                        self.logger.error("Invalid timestamp values detected")
                        raise ValueError("Invalid timestamp values in data")
                    
                    # Additional timestamp validation
                    min_timestamp = numeric_timestamps.min()
                    max_timestamp = numeric_timestamps.max()
                    self.logger.debug(f"Timestamp range: {min_timestamp} to {max_timestamp}")
                        
                    # Convert Unix millisecond timestamp to datetime for grouping
                    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
                    self.logger.debug(f"Converted dates: {df['date'].head()}")
                    
                    # Group by date and category, ensuring consistent ordering
                    df = df.groupby([df['date'].dt.date, 'category'])['value'].sum().reset_index()
                    df = df.sort_values(['date', 'category'])
                    
                    # Convert back to Unix timestamp for Grafana
                    df['timestamp'] = df['date'].apply(
                        lambda x: int(datetime.combine(x, datetime.min.time()).timestamp() * 1000)
                    )
                    
                    # Verify final timestamps
                    self.logger.debug(f"Final timestamp range: {df['timestamp'].min()} to {df['timestamp'].max()}")

                    # Drop the intermediate date column
                    df = df.drop('date', axis=1)
                    time_series = df.to_dict('records')
                    self.logger.debug("Time series processing completed successfully")
                    self.logger.debug(f"Sample processed data: {time_series[:2] if time_series else []}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing timestamps: {str(e)}")
                    self.logger.error(f"Sample problematic data: {df['timestamp'].head() if 'timestamp' in df else 'No timestamp column'}")
                    raise

            # Process themes deterministically
            theme_counts = pd.Series(analysis_results['themes']).value_counts()
            significant_themes = theme_counts[theme_counts > 1].head(20)

            formatted_data = {
                "categories": sorted(list(self.categories.keys())),
                "time_series": time_series,
                "category_totals": dict(sorted(analysis_results['category_counts'].items())),
                "top_themes": dict(sorted(significant_themes.items(), key=lambda x: (-x[1], x[0])))
            }
            
            # Validate the formatted data
            self._validate_formatted_data(formatted_data)
            
            # Log analysis results
            self.logger.info(f"Analysis completed with {len(time_series)} time series points")
            self.logger.debug(f"Categories: {formatted_data['categories']}")
            self.logger.debug(f"Sample time series: {time_series[:2] if time_series else []}")
            
            return formatted_data
            
        except Exception as e:
            self.logger.error(f"Error in analysis: {str(e)}")
            self.logger.error("Full traceback:", exc_info=True)
            raise            

if __name__ == "__main__":
    analyzer = EmailSubjectAnalyzer()
    results = analyzer.run_analysis()
    print(results)