import pandas as pd
import requests
from scripts.config import Config

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):
    """
    Load data from StreamsCharts API
    """
    config = Config()
    client_id = config.STREAMS_CHARTS_CLIENT_ID
    token = config.STREAMS_CHARTS_TOKEN
    url = "https://streamscharts.com/api/jazz/channels?platform=twitch&time=7-days"
    headers = {"Client-ID": client_id, "Token": token.strip("'")}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error for bad status codes
    
    json_response = response.json()
    
    # Check if 'data' exists in response
    if 'data' not in json_response:
        raise KeyError(f"Expected 'data' in response. Got keys: {list(json_response.keys())}")
    
    return pd.DataFrame(json_response['data'])