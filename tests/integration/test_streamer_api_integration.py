# tests/integration/test_streamer_api.py
import pytest
from data_loaders.load_streamer_data import load_data

@pytest.mark.integration
@pytest.mark.api
def test_streamer_api_integration():
    """Test that the data is loaded correctly from the Streamer API"""

    df = load_data()

    assert len(df) > 0
    print(f"\nSuccessfully loaded {len(df)} records from API")
    
    # Basic structure check
    assert 'platform' in df.columns
    assert 'channel_name' in df.columns
    assert 'hours_watched' in df.columns