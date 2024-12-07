# tests/integration/test_data_exporters.py
import pytest
import duckdb
from scripts.config import Config
from data_exporters.export_to_md import export_data

from unittest.mock import patch
@pytest.mark.integration
class TestDataExporter:
    def setup_method(self):
        """Setup method to initialise before each test"""
        self.settings = Config(DESTINATION='md')
        
    def teardown_method(self):
        """Clean up after each test"""
        with duckdb.connect("md:") as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {self.settings.database_name}")

    def test_export_data_integration(self, sample_data):
        """Test the complete export_data function with real database"""
        with patch('data_exporters.export_to_md.Config') as mock_config:
            mock_config.return_value = self.settings
            export_data(sample_data)

        
        with duckdb.connect("md:") as conn:
            conn.execute(f"USE {self.settings.database_name}")
            # Verify that the data was inserted correctly
            result = conn.execute(f"SELECT * FROM {self.settings.table_name}").fetchall()
            assert len(result) == len(sample_data)

            # Check data accuracy
            result = conn.execute(f"""
                SELECT platform, channel_name, hours_watched 
            FROM {self.settings.table_name}
                WHERE channel_name = 'channel1'
            """).fetchone()

            assert result[0] == 'twitch'
            assert result[1] == 'channel1'
            assert result[2] == 1000

    def test_export_data_multiple_runs(self, sample_data):
        """Test that the data is appended correctly when run multiple times"""
        with patch('data_exporters.export_to_md.Config') as mock_config:
            mock_config.return_value = self.settings
            export_data(sample_data)
            export_data(sample_data)

        with duckdb.connect("md:") as conn:
            conn.execute(f"USE {self.settings.database_name}")
            result = conn.execute(f"SELECT * FROM {self.settings.table_name}").fetchall()
            assert len(result) == len(sample_data) * 2
