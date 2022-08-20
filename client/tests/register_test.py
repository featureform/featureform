import os
import shutil
import stat
import sys

sys.path.insert(0, 'client/src/')
import pytest
from featureform.register import LocalProvider, Provider, Registrar, LocalConfig
from featureform.resources import SQLTransformation, Source, DFTransformation

@pytest.fixture
def local():
    config = LocalConfig()
    provider = Provider(name="local-mode",
                    function="LOCAL_ONLINE",
                    description="This is local mode",
                    team="team",
                    config=config)
    return LocalProvider(Registrar(), provider)

@pytest.fixture
def registrar():
    return Registrar()

def name():
    """doc string"""
    return "query"

def empty_string():
    return ""

def return_5():
    return 5

@pytest.mark.parametrize("fn", [empty_string, return_5])
def test_sql_transformation_decorator_invalid_fn(local, fn):
    decorator = local.sql_transformation(
        variant="var",
        owner="owner"
    )
    with pytest.raises((TypeError, ValueError)):
        decorator(fn)

def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)
    
@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Remove any lingering Databases
    try:
        shutil.rmtree('.featureform', onerror=del_rw)
    except:
        print("File Already Removed")
    yield
    try:
        shutil.rmtree('.featureform', onerror=del_rw)
    except:
        print("File Already Removed")