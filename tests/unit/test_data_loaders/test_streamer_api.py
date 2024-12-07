import pytest
import requests
from unittest.mock import patch


def test_fetch_channels(api_client):
    assert api_client.client_id == "test_client_id"
    assert api_client.token == "test_token"
    assert api_client.base_url == "https://streamscharts.com/api/jazz"

def test_get_headers(api_client):
    headers = api_client.get_headers()
    assert headers == {
        "Client-ID": "test_client_id",
        "Token": "test_token",
        "Accept": "application/json"
    }

def test_fetch_channels_success(api_client, mock_streamer_data):
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_streamer_data
        mock_get.return_value.raise_for_status = lambda: None
        response = api_client.fetch_channels()
        assert response == mock_streamer_data

@pytest.mark.parametrize("exception_class", [requests.Timeout, requests.RequestException])
def test_fetch_channels_timeout(api_client, exception_class):
    with patch("requests.get") as mock_get:
        mock_get.side_effect = exception_class
        with pytest.raises(exception_class):
            api_client.fetch_channels()