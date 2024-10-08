import pytest
import datetime
from google.cloud import bigquery
from mig_dx_api import CreatedBy, Installation
from uuid import UUID, uuid4
from unittest.mock import patch, MagicMock

from dxapi_portal_runner.main import (
    get_target_installation,
    get_schema,
    # create_dataset,
    # get_source_data,
    # write_data_to_file,
    # main
)

TEST_APP_ID = str(uuid4())
TEST_PRIVATE_KEY = 'private_key_contents'
TEST_CREATOR = CreatedBy(
    user_id = 123,
    display_name = 'Test User',
    email = 'test@example.com')

# @pytest.fixture
# def dx_client(mocker):
#     mock_client = MagicMock()
#     mocker.patch('client.get_installations', mock_client)

def test_get_target_installation_default():
    installations = [
        Installation(
            movement_app_id = str(uuid4()),
            installation_id = 1,
            workspace_id = 100,
            created_by = TEST_CREATOR,
            date_created = datetime.datetime.now(),
            name = 'Test Installation'
        )
    ]

    target_installation = get_target_installation(installations)
    
    assert target_installation == installations[0]

def test_get_target_installation_with_install_id_argument():
    installations = [
        Installation(
            movement_app_id = str(uuid4()),
            installation_id = 1,
            workspace_id = 100,
            created_by = TEST_CREATOR,
            date_created = datetime.datetime.now(),
            name = 'Test Installation'
        ),
        Installation(
            movement_app_id = str(uuid4()),
            installation_id = 31,
            workspace_id = 100,
            created_by = TEST_CREATOR,
            date_created = datetime.datetime.now(),
            name = 'Test Installation 2'
        )
    ]
    target_installation = get_target_installation(installations, '31')
    
    assert target_installation == installations[1]

def test_get_target_installation_no_installations():
    with pytest.raises(Exception):
        installations = []
        get_target_installation(installations)

def test_get_target_installation_with_installation_id_not_found():
    with pytest.raises(Exception):
        installations = [
            Installation(
                movement_app_id = str(uuid4()),
                installation_id = 1,
                workspace_id = 100,
                created_by = TEST_CREATOR,
                date_created = datetime.datetime.now(),
                name = 'Test Installation'
            ),
            Installation(
                movement_app_id = str(uuid4()),
                installation_id = 31,
                workspace_id = 100,
                created_by = TEST_CREATOR,
                date_created = datetime.datetime.now(),
                name = 'Test Installation 2'
            )
        ]
        get_target_installation(installations, '21')

@patch('google.cloud.bigquery.Client', autospec=True)
def test_get_schema(mock_bigquery):
    primary_key = 'van_id'
    mock_bigquery.get_table('project.dataset.test_table').schema = [
        bigquery.SchemaField(primary_key, 'INT64'),
        bigquery.SchemaField('first_name', 'STRING'),
        bigquery.SchemaField('last_name', 'STRING'),
        bigquery.SchemaField('city', 'STRING', 'NULLABLE'),
        bigquery.SchemaField('state', 'STRING', 'NULLABLE')
    ]

    mock_bigquery.get_table('project.dataset.test_table').table_constraints.primary_key.columns = [
        primary_key
    ]

    schema = get_schema(mock_bigquery, 'test_table', 'dataset', 'project')
    print(f'SCHEMA: {schema}')
    assert len(schema.properties) == 5
    assert schema.primary_key == [primary_key]
    assert schema.properties[0].type == "string"


# def test_create_dataset():
#     assert False

# def test_get_source_data():
#     # row data coming back from bigquery is a string, not json
#     assert False

# def test_write_data_to_file():
#     assert False

# def test_main():
#     # dataset doesn't exist
#     # dataset does exist
#     assert False
