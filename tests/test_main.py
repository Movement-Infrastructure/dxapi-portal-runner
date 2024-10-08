import pytest
from mig_dx_api import DX, Installation
from uuid import UUID, uuid4
from src.dxapi_portal_runner import (
    get_target_installation
    # get_schema,
    # create_dataset,
    # get_source_data,
    # write_data_to_file,
    # main
)

TEST_APP_ID = str(uuid4())
TEST_PRIVATE_KEY = "private_key_contents"

@pytest.fixture
def dx_client(mocker):
    client = DX(app_id=TEST_APP_ID, private_key=TEST_PRIVATE_KEY)
    yield client


def test_get_target_installation_default(dx_client: DX):
    installations = dx_client.get_installations()
    target_installation = get_target_installation(installations)
    assert False

def test_get_target_installation_with_argument():
    assert False

def test_get_target_installation_no_installations():
    assert False

def test_get_target_installation_with_argument_not_found():
    assert False

def test_get_schema():
    assert False

def test_create_dataset():
    assert False

def test_get_source_data():
    # row data coming back from bigquery is a string, not json
    assert False

def test_write_data_to_file():
    assert False

def test_main():
    # dataset doesn't exist
    # dataset does exist
    assert False
