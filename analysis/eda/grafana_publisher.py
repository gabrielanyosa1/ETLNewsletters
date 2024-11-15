import requests
import json
import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from dateutil import parser
from config.grafana_config import GrafanaConfig, load_grafana_config

class JSONLogger:
    """Custom JSON logger for detailed troubleshooting."""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # Create a file handler for JSON logs
        json_handler = logging.FileHandler('grafana_debug.json')
        json_handler.setLevel(logging.DEBUG)
        
        # Create a formatter
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
            '"module": "%(module)s", "function": "%(funcName)s", '
            '"line": %(lineno)d, "message": %(message)s}'
        )
        
        json_handler.setFormatter(formatter)
        self.logger.addHandler(json_handler)
    
    def log_dict(self, message: str, data: Dict) -> None:
        """Log a dictionary as a JSON string."""
        self.logger.debug(f'"{message}": {json.dumps(data, default=str)}')

class GrafanaPublisher:
    def __init__(self, config: Optional[GrafanaConfig] = None):
        self.config = config or load_grafana_config()
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.config.service_account_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        self.logger = logging.getLogger(__name__)

    def get_or_create_folder(self, folder_name: str) -> Optional[str]:
        """Get existing folder or create a new one."""
        try:
            # First, try to find existing folder
            response = self.session.get(f"{self.config.instance_url}/api/folders")
            response.raise_for_status()
            folders = response.json()
            
            # Check if folder exists
            for folder in folders:
                if folder['title'] == folder_name:
                    self.logger.info(f"Found existing folder: {folder_name}")
                    return folder['uid']
            
            # If folder doesn't exist, create it
            self.logger.info(f"Creating new folder: {folder_name}")
            response = self.session.post(
                f"{self.config.instance_url}/api/folders",
                json={"title": folder_name}
            )
            response.raise_for_status()
            folder_data = response.json()
            return folder_data['uid']
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error managing folder: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                self.logger.error(f"Response content: {e.response.text}")
            return None

    def test_connection(self) -> bool:
        """Test connection to Grafana."""
        try:
            response = self.session.get(f"{self.config.instance_url}/api/org")
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False

    def validate_timestamp(self, timestamp: Union[str, float, int]) -> bool:
        """Validate timestamp format."""
        try:
            if isinstance(timestamp, (int, float)):
                # For millisecond timestamps, convert to seconds first
                seconds = timestamp / 1000 if timestamp > 1e10 else timestamp
                datetime.fromtimestamp(seconds)
            else:
                parser.parse(timestamp)
            return True
        except (ValueError, TypeError) as e:
            self.logger.error(f"Invalid timestamp format: {timestamp} - {str(e)}")
            return False

    def validate_data(self, data: Dict) -> bool:
        """Validate data structure before publishing."""
        required_fields = ['categories', 'time_series', 'category_totals', 'top_themes']
        
        # Check required fields
        if not all(field in data for field in required_fields):
            missing = [field for field in required_fields if field not in data]
            self.logger.error(f"Missing required fields: {missing}")
            return False

        # Validate categories
        if not isinstance(data['categories'], (list, tuple)):
            self.logger.error("Categories must be a list or tuple")
            return False
            
        # Validate time series data structure
        if data['time_series']:
            self.logger.debug(f"Validating {len(data['time_series'])} time series entries")
            for item in data['time_series']:
                # Check required keys
                if not all(key in item for key in ['timestamp', 'category', 'value']):
                    self.logger.error(f"Invalid time series entry: {item}")
                    return False
                
                # Validate timestamp with enhanced logging
                timestamp = item['timestamp']
                if isinstance(timestamp, (int, float)):
                    if timestamp <= 0:
                        self.logger.error(f"Invalid timestamp value (<=0): {timestamp}")
                        return False
                    # Additional check for reasonable timestamp range
                    seconds = timestamp / 1000 if timestamp > 1e10 else timestamp
                    try:
                        datetime.fromtimestamp(seconds)
                    except (ValueError, OSError) as e:
                        self.logger.error(f"Invalid timestamp value: {timestamp} - {str(e)}")
                        return False
                else:
                    if not self.validate_timestamp(timestamp):
                        return False
                
                # Validate category
                if item['category'] not in data['categories']:
                    self.logger.error(f"Unknown category in time series: {item['category']}")
                    return False
                
                # Validate value
                try:
                    float(item['value'])
                except (ValueError, TypeError):
                    self.logger.error(f"Invalid value in time series: {item['value']}")
                    return False

        # Validate category totals
        if not isinstance(data['category_totals'], dict):
            self.logger.error("Category totals must be a dictionary")
            return False
        
        for category, total in data['category_totals'].items():
            if category not in data['categories']:
                self.logger.error(f"Unknown category in totals: {category}")
                return False
            try:
                float(total)
            except (ValueError, TypeError):
                self.logger.error(f"Invalid total for category {category}: {total}")
                return False

        # Validate top themes
        if not isinstance(data['top_themes'], dict):
            self.logger.error("Top themes must be a dictionary")
            return False
        
        for theme, count in data['top_themes'].items():
            try:
                float(count)
            except (ValueError, TypeError):
                self.logger.error(f"Invalid count for theme {theme}: {count}")
                return False

        # Log successful validation
        self.logger.info("Data validation passed with:")
        self.logger.info(f"- Categories: {len(data['categories'])}")
        self.logger.info(f"- Time series points: {len(data['time_series'])}")
        self.logger.info(f"- Top themes: {len(data['top_themes'])}")
        
        # Additional debug logging for timestamps
        if data['time_series']:
            timestamps = [entry['timestamp'] for entry in data['time_series']]
            self.logger.debug(f"Timestamp range: {min(timestamps)} to {max(timestamps)}")
        
        return True

    def format_time_series_data(self, time_series_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format time series data with improved handling."""
        self.logger.info("Starting time series formatting")
        
        # Group by category and timestamp
        formatted_series = {}
        
        for entry in time_series_data:
            try:
                category = entry['category']
                timestamp = entry['timestamp']
                
                # Consistent timestamp handling
                if isinstance(timestamp, str):
                    dt = parser.parse(timestamp)
                    timestamp_ms = int(dt.timestamp() * 1000)
                else:
                    timestamp_ms = int(float(timestamp) * 1000)
                
                if category not in formatted_series:
                    formatted_series[category] = []
                
                formatted_series[category].append([float(entry['value']), timestamp_ms])
                
            except Exception as e:
                self.logger.error(f"Error processing time series entry: {str(e)}")
                continue

        # Convert to Grafana series format with proper naming
        series_data = []
        for category, points in sorted(formatted_series.items()):
            sorted_points = sorted(points, key=lambda x: x[1])
            series_data.append({
                "target": category.capitalize(),
                "datapoints": sorted_points,
                "refId": category,
                "type": "timeseries",
                "legendFormat": category.capitalize()
            })
            self.logger.debug(f"Created series for {category} with {len(points)} points")

        return series_data

    def format_category_data(self, category_totals: Dict[str, int]) -> List[Dict[str, Any]]:
        """Format category data for pie chart with proper naming."""
        timestamp_ms = int(datetime.now().timestamp() * 1000)
        
        # Sort categories for consistency
        sorted_categories = sorted(category_totals.items(), key=lambda x: (-x[1], x[0]))
        
        return [
            {
                "target": category,  # Use actual category name
                "datapoints": [[float(count), timestamp_ms]],
                "refId": category,
                "legendFormat": category.capitalize(),  # Proper display name
            }
            for category, count in sorted_categories
        ]

    def format_theme_data(self, themes: Dict[str, int]) -> List[Dict[str, Any]]:
        """Format theme data for bar gauge."""
        timestamp_ms = int(datetime.now().timestamp() * 1000)
        return [
            {
                "target": theme,
                "datapoints": [[float(count), timestamp_ms]],
                "refId": theme,
                "legendFormat": theme
            }
            for theme, count in sorted(themes.items(), key=lambda x: (-x[1], x[0]))
        ]

    def _create_time_series_panel(self, time_series: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create time series panel configuration."""
        return {
            "id": 1,
            "title": "Newsletter Categories Over Time",
            "type": "timeseries",
            "gridPos": {"h": 8, "w": 24, "x": 0, "y": 0},
            "datasource": {"type": "grafana", "uid": "grafana"},
            "fieldConfig": {
                "defaults": {
                    "custom": {
                        "drawStyle": "line",
                        "lineWidth": 2,
                        "fillOpacity": 20,
                        "pointSize": 5,
                        "lineInterpolation": "smooth",
                        "spanNulls": True,
                        "showPoints": "auto"
                    },
                    "min": 0,
                    "unit": "short",
                    "color": {"mode": "palette-classic"},
                    "displayName": "${__field.labels.target}"
                }
            },
            "options": {
                "tooltip": {"mode": "multi"},
                "legend": {"displayMode": "table", "placement": "bottom"}
            },
            "targets": time_series
        }

    def _create_pie_chart_panel(self, category_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create pie chart panel configuration."""
        return {
            "id": 2,
            "title": "Newsletter Category Distribution",
            "type": "piechart",
            "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
            "datasource": {"type": "grafana", "uid": "grafana"},
            "fieldConfig": {
                "defaults": {
                    "custom": {
                        "hideFrom": {"tooltip": False, "viz": False, "legend": False}
                    },
                    "mappings": [],
                    "color": {"mode": "palette-classic"}
                }
            },
            "options": {
                "legend": {
                    "displayMode": "table",
                    "placement": "right",
                    "showLegend": True,
                    "values": ["value", "percent"]
                },
                "pieType": "pie",
                "reduceOptions": {
                    "values": True,
                    "calcs": ["lastNotNull"],
                    "fields": ""
                },
                "tooltip": {"mode": "single", "sort": "none"}
            },
            "targets": category_data
        }

    def _create_bar_gauge_panel(self, theme_data: List[Dict[str, Any]], themes: Dict[str, int]) -> Dict[str, Any]:
        """Create bar gauge panel configuration."""
        return {
            "id": 3,
            "title": "Most Common Newsletter Themes",
            "type": "bargauge",
            "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
            "datasource": {"type": "grafana", "uid": "grafana"},
            "fieldConfig": {
                "defaults": {
                    "min": 0,
                    "max": max(themes.values()) if themes else 100,
                    "thresholds": {
                        "mode": "percentage",
                        "steps": [
                            {"color": "blue", "value": None},
                            {"color": "green", "value": 50},
                            {"color": "red", "value": 80}
                        ]
                    },
                    "displayName": "${__field.labels.target}",
                    "mappings": []
                }
            },
            "options": {
                "orientation": "horizontal",
                "displayMode": "gradient",
                "showUnfilled": True,
                "valueMode": "color"
            },
            "targets": theme_data
        }

    def create_email_analysis_dashboard(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create or update dashboard with fixed category visualization."""
        self.logger.info("Creating email analysis dashboard")

        # Validate data structure
        if not self.validate_data(analysis_data):
            raise ValueError("Invalid analysis data structure")
        
        try:
            # Format data consistently
            time_series = self.format_time_series_data(analysis_data['time_series'])
            category_data = self.format_category_data(analysis_data['category_totals'])
            theme_data = self.format_theme_data(analysis_data['top_themes'])
            
            # Log data for verification
            self.logger.info(f"Categories being visualized: {sorted(analysis_data['category_totals'].keys())}")
            self.logger.info(f"Category data points: {[d['target'] for d in category_data]}")
            self.logger.info(f"Time series categories: {sorted(set(d['target'] for d in time_series))}")

            dashboard = {
                "dashboard": {
                    "id": None,
                    "uid": "email-analysis-dashboard",
                    "title": "Email Newsletter Analysis",
                    "tags": ["email-analysis"],
                    "timezone": "browser",
                    "refresh": "",  # Disable auto-refresh
                    "panels": [
                        self._create_time_series_panel(time_series),
                        self._create_pie_chart_panel(category_data),
                        self._create_bar_gauge_panel(theme_data, analysis_data['top_themes'])
                    ],
                    "time": {
                        "from": "2023-01-01T00:00:00.000Z",
                        "to": "2025-01-01T00:59:59.000Z"
                    },
                    "timepicker": {
                        "refresh_intervals": [],  # Disable refresh intervals
                         "hidden": True  # Hide refresh controls
                    },
                    "schemaVersion": 36,
                    "version": 1,
                    "iteration": 1
                },
                "message": "Updated dashboard",
                "folderId": 0,
                "overwrite": True
            }

            response = self.session.post(
                f"{self.config.instance_url}/api/dashboards/db",
                json=dashboard
            )
            response.raise_for_status()
            self.logger.info("Dashboard created successfully")
            return response.json()
            
        except Exception as e:
            self.logger.error(f"Dashboard creation failed: {str(e)}")
            if hasattr(e, 'response'):
                self.logger.error(f"Response text: {e.response.text}")
            raise

    def delete_dashboard(self, uid: str) -> bool:
        """Delete a dashboard by its UID.
        
        Args:
            uid (str): Dashboard UID to delete.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            response = self.session.delete(
                f"{self.config.instance_url}/api/dashboards/uid/{uid}"
            )
            response.raise_for_status()
            self.logger.info(f"Successfully deleted dashboard with UID: {uid}")
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to delete dashboard: {str(e)}")
            return False