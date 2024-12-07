from mage_ai.data_preparation.decorators import pipeline
from data_loaders.load_streamer_data import load_data
from transformers.transform_streamer_data import transform
from data_exporters.export_to_md import export_data

@pipeline
def weekly_streamer_stats():
    """
    Pipeline to load, transform, and export streamer data
    """
    data = load_data()
    transformed_data = transform(data)
    export_data(transformed_data)