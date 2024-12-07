from unittest.mock import patch, MagicMock
import pytest
from data_exporters.export_to_md import export_data
import duckdb

@patch('data_exporters.export_to_md.DuckDBDataIngestor')
def test_export_data(mock_loader, sample_data):
    """Test successful data export"""
    mock_loader_instance = MagicMock()
    mock_loader.return_value = mock_loader_instance

    export_data(sample_data)

    mock_loader.assert_called_once()
    mock_loader_instance.setup_schema.assert_called_once()
    mock_loader_instance.insert_data.assert_called_once()


@patch('data_exporters.export_to_md.DuckDBDataIngestor')
def test_export_data_handles_loader_error(mock_loader, sample_data):
    """Test that export_data properly handles database errors"""
    mock_loader_instance = MagicMock()
    mock_loader_instance.insert_data.side_effect = duckdb.Error("Database error")
    mock_loader.return_value = mock_loader_instance

    with pytest.raises(RuntimeError, match="Failed to insert data: Database error"):
        export_data(sample_data)