import uuid
from scripts.config import Config

settings = Config()

def get_test_table_name(base_name):
    if settings.ENVIRONMENT == "test":
        return f"{base_name}_{uuid.uuid4().hex[:8]}"
    return base_name