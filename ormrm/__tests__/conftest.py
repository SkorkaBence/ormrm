import pytest

from .fake_services import reset_call_counters


@pytest.fixture(autouse=True)
def _reset_counters():
    reset_call_counters()
    yield
    reset_call_counters()
