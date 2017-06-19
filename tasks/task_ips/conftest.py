import pytest
from ip import DBConnector
from ip_test_utils import DBConnectorTester

def pytest_addoption(parser):
    parser.addoption("--mongo", default='mongodb://127.0.0.1:27017')
    parser.addoption("--db", default='mytest')


@pytest.fixture(scope='session')
def mongo_url(request):
    return request.config.getoption("--mongo")


@pytest.fixture(scope='session')
def mongo_db(request):
    return request.config.getoption("--db")


@pytest.fixture
def db_connector(mongo_url, mongo_db):
    return DBConnectorTester(DBConnector(mongo_url, mongo_db))
