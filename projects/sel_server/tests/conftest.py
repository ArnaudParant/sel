import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="session")
def api():
    from sel_server.routes import app
    return TestClient(app)
