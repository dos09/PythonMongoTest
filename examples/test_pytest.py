import pytest

def inc(x):
    return x + 1

def test_inc():
    assert inc(3) == 4

# Window -> Preferences -> PyDev -> PyUnit -> TestRunner -> Py.test runner    
# Run as -> Python unit test    
