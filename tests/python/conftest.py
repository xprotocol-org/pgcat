import pytest
from utils import pgcat_start, pg_cat_send_signal
import signal


@pytest.fixture(scope="function")
def pgcat():
    pgcat_start()
    yield
    pg_cat_send_signal(signal.SIGTERM)

