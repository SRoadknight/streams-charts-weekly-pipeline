import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(data, *args, **kwargs):
    """
    Add weekly date ranges and identifiers
    """
    start_date = datetime.datetime.today().date() - datetime.timedelta(days=datetime.datetime.today().weekday())
    end_date = start_date + datetime.timedelta(days=6)
    week_number = start_date.isocalendar()[1]
    year = start_date.year

    data['start_date'] = start_date
    data['end_date'] = end_date
    data['year'] = year
    data['week_number'] = week_number
    return data