import pytest
from starter import get_api

@pytest.fixture(scope="session")
def sel():
    return get_api()
