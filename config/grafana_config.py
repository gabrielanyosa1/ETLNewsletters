from dataclasses import dataclass
from typing import Optional
import os
from dotenv import load_dotenv

@dataclass
class GrafanaConfig:
    """Grafana Cloud configuration."""
    instance_url: str
    service_account_token: str
    org_id: Optional[int] = None
    default_dashboard_folder: Optional[str] = "Email Analysis"

def load_grafana_config() -> GrafanaConfig:
    """Load Grafana configuration from environment variables."""
    load_dotenv()
    
    required_vars = ['GRAFANA_INSTANCE_URL', 'GRAFANA_SA_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return GrafanaConfig(
        instance_url=os.getenv('GRAFANA_INSTANCE_URL'),
        service_account_token=os.getenv('GRAFANA_SA_TOKEN'),
        org_id=int(os.getenv('GRAFANA_ORG_ID', '1')),
        default_dashboard_folder=os.getenv('GRAFANA_DASHBOARD_FOLDER', 'Email Analysis')
    )