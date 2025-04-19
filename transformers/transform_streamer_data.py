import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(data, *args, **kwargs):
    """
    Add weekly date ranges and identifiers
    """
    today = datetime.datetime.today()
    days_since_monday = today.weekday() + 7 
    start_date = today - datetime.timedelta(days=days_since_monday)
    end_date = start_date + datetime.timedelta(days=6)
    year, week_number, _ = start_date.isocalendar()

    data['start_date'] = start_date
    data['end_date'] = end_date
    data['data_year'] = year
    data['week_number'] = week_number
    data['rank_within_week'] = data['hours_watched'].rank(method='dense', ascending=False).astype(int)
    return data