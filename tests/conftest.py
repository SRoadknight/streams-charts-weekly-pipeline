import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import date
@pytest.fixture
def sample_data():
    sample_data = pd.DataFrame({
        "platform": ["twitch", "youtube"],
        "channel_name": ["channel1", "channel2"],
        "channel_display_name": ["Channel One", "Channel Two"],
        "channel_id": ["123", "456"],
        "hours_watched": [1000, 2000],
        "peak_viewers": [150, 300],
        "average_viewers": [75, 150],
        "airtime_in_m": [120, 240],
        "followers_gain": [10, 20],
        "live_views": ["1000", "2000"],
        "last_streamed_game": ["Game A", "Game B"],
        "avatar_url": ["http://example.com/avatar1.png", "http://example.com/avatar2.png"],
        "channel_country": ["US", "CA"],
        "stream_language": ["en", "fr"],
        "partnership_status": ["partner", "affiliate"],
        "channel_type": ["Male", "Female"],
        "start_date": [date(2024, 1, 1), date(2024, 1, 8)],
        "end_date": [date(2024, 1, 7), date(2024, 1, 14)],
        "year": [2024, 2024],
        "week_number": [1, 2]
    })
    return sample_data



@pytest.fixture
def mock_streamer_data():
    return {
        "data": [
            {
                "platform": "twitch",
                "channel_name": "channel_1",
                "channel_display_name": "Channel One",
                "channel_id": "123456789",
                "hours_watched": 1000000,
                "peak_viewers": 50000,
                "average_viewers": 20000,
                "airtime_in_m": 3000,
                "followers_gain": 10000,
                "live_views": None,
                "last_streamed_game": "Game A",
                "avatar_url": "https://example.com/avatar1.png",
                "channel_country": "US",
                "stream_language": "en",
                "partnership_status": "partner",
                "channel_type": "Male"
            },
            {
                "platform": "twitch",
                "channel_name": "channel_2",
                "channel_display_name": "Channel Two",
                "channel_id": "987654321",
                "hours_watched": 2000000,
                "peak_viewers": 60000,
                "average_viewers": 25000,
                "airtime_in_m": 4000,
                "followers_gain": 15000,
                "live_views": None,
                "last_streamed_game": "Game B",
                "avatar_url": "https://example.com/avatar2.png",
                "channel_country": "BR",
                "stream_language": "pt",
                "partnership_status": "partner",
                "channel_type": "Female"
            }
        ]
    }

@pytest.fixture
def api_client():
    from data_loaders.load_streamer_data import StreamerAPIClient
    return StreamerAPIClient(
        client_id="test_client_id",
        token="test_token"
    )


@pytest.fixture
def mock_config():
    class MockConfig:
        STREAMS_CHARTS_CLIENT_ID = "test_client_id"
        STREAMS_CHARTS_TOKEN = "test_token"
    return MockConfig()
