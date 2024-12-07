import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from data_loaders.load_streamer_data import load_data


def test_load_data_success(mock_streamer_data, mock_config):
    """Test successful data loading"""
    with patch('data_loaders.load_streamer_data.Config', return_value=mock_config):
        with patch('data_loaders.load_streamer_data.StreamerAPIClient') as MockClient:
            mock_client = MagicMock()
            mock_client.fetch_channels.return_value = mock_streamer_data
            MockClient.return_value = mock_client

            result = load_data()

            assert isinstance(result, pd.DataFrame)
            assert not result.empty
            assert len(result) == len(mock_streamer_data["data"])
            assert list(result.columns) == [
                "platform", "channel_name", "channel_display_name", 
                "channel_id", "hours_watched", "peak_viewers", 
                "average_viewers", "airtime_in_m", "followers_gain", 
                "live_views", "last_streamed_game", "avatar_url", 
                "channel_country", "stream_language", "partnership_status", 
                "channel_type"
            ]

def test_load_data_missing_credentials():
    """Test handling of missing credentials"""
    with patch('data_loaders.load_streamer_data.Config') as MockConfig:
        MockConfig.return_value.STREAMS_CHARTS_CLIENT_ID = None
        MockConfig.return_value.STREAMS_CHARTS_TOKEN = None

        with pytest.raises(ValueError, match="Missing required credentials"):
            load_data()

def test_load_data_api_error(mock_config):
    """Test handling of API errors"""
    with patch('data_loaders.load_streamer_data.Config') as MockConfig:
        MockConfig.return_value = mock_config

        with patch('data_loaders.load_streamer_data.StreamerAPIClient') as MockClient:
            mock_client = MagicMock()
            mock_client.fetch_channels.side_effect = Exception("API Error")
            MockClient.return_value = mock_client

            with pytest.raises(Exception, match="API Error"):
                load_data()