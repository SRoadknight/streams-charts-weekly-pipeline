from datetime import date
from freezegun import freeze_time
from transformers.transform_streamer_data import transform


@freeze_time("2024-12-09")  # Thursday
def test_transform_adds_correct_dates(sample_data):
    """Test that correct dates are added to the DataFrame"""
    result = transform(sample_data)
    
    # Expected dates for week containing March 14, 2024
    expected_start = date(2024, 12, 2)  # Monday
    expected_end = date(2024, 12, 8)    # Sunday
    
    assert result['start_date'].iloc[0].date() == expected_start
    assert result['end_date'].iloc[0].date() == expected_end
    assert result['data_year'].iloc[0] == 2024
    assert result['week_number'].iloc[0] == 49

@freeze_time("2025-01-06")  # Testing year boundary
def test_transform_year_boundary(sample_data):
    """Test transformation during year boundary"""
    result = transform(sample_data)
    
    assert result['year'].iloc[0] == 2025
    assert result['week_number'].iloc[0] == 1

@freeze_time("2021-01-04")
def test_transform_year_boundary_2021(sample_data):
    """Test transformation during year boundary"""
    result = transform(sample_data)
    
    assert result['year'].iloc[0] == 2020
    assert result['week_number'].iloc[0] == 53

def test_transform_preserves_original_data(sample_data):
    """Test that original data columns are preserved"""
    result = transform(sample_data)
    
    # Check original columns exist and values unchanged
    assert 'platform' in result.columns
    assert 'channel_name' in result.columns
    assert 'hours_watched' in result.columns
    assert result['platform'].tolist() == sample_data['platform'].tolist()
    assert result['channel_name'].tolist() == sample_data['channel_name'].tolist()
    assert result['hours_watched'].tolist() == sample_data['hours_watched'].tolist()

def test_transform_adds_required_columns(sample_data):
    """Test that all required columns are added"""
    result = transform(sample_data)
    
    required_columns = ['start_date', 'end_date', 'year', 'week_number']
    for col in required_columns:
        assert col in result.columns