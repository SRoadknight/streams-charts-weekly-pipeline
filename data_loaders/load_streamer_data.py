import pandas as pd
import requests
from typing import Dict, Any
from scripts.config import Config

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


class StreamerAPIClient:
    """Handle API connection and requests"""
    def __init__(self, client_id: str, token: str):
        self.client_id = client_id
        self.token = token.strip("'")
        self.base_url = "https://streamscharts.com/api/jazz"
    
    def get_headers(self) -> Dict[str, str]:
        return {
            "Client-ID": self.client_id,
            "Token": self.token,
            "Accept": "application/json"
        }
    
    def fetch_channels(self, platform: str = "twitch", time_range: str = "7-days") -> Dict[str, Any]:
        """Fetch channel data from API"""
        url = f"{self.base_url}/channels"
        params = {
            "platform": platform,
            "time": time_range
        }
        
        try:
            response = requests.get(
                url,
                params=params,
                headers=self.get_headers(),
                timeout=10
            )
            response.raise_for_status()
            return response.json()
            
        except requests.Timeout:
            raise 
        except requests.RequestException as e:
            raise

@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """
    Load raw data from StreamsCharts API
    
    Returns:
        pd.DataFrame: Raw streamer data
    """
    settings = Config()
    
    if not settings.STREAMS_CHARTS_CLIENT_ID or not settings.STREAMS_CHARTS_TOKEN:
        raise ValueError("Missing required credentials")
    
    client = StreamerAPIClient(
        client_id=settings.STREAMS_CHARTS_CLIENT_ID,
        token=settings.STREAMS_CHARTS_TOKEN
    )
    
    try:
        response_data = client.fetch_channels()
        return pd.DataFrame(response_data.get('data', []))
        
    except Exception as e:
        raise