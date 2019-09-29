import square.square


def pytest_configure(*args, **kwargs):
    """Pytest calls this hook on startup."""
    # Set log level to DEBUG for all unit tests.
    square.square.setup_logging(9)
