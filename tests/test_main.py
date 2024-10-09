import pytest
import datetime
from google.cloud import bigquery
from mig_dx_api import CreatedBy, Installation
from uuid import UUID, uuid4
from unittest import mock
from unittest.mock import patch

from dxapi_portal_runner.main import (
    get_target_installation,
    get_schema,
    get_source_data,
    # write_data_to_file,
    # main
)

TEST_APP_ID = str(uuid4())
TEST_PRIVATE_KEY = 'private_key_contents'
TEST_CREATOR = CreatedBy(
    user_id = 123,
    display_name = 'Test User',
    email = 'test@example.com')

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

@patch('google.cloud.bigquery.Client', autospec=True)
def test_get_source_data(mock_bigquery):
    mock_query_job = mock.create_autospec(bigquery.QueryJob)
    # row data coming back from bigquery is a string, not json
    mock_rows = mock.create_autospec(bigquery.table.RowIterator)
    mock_rows.__iter__.return_value = [
        bigquery.Row(('{"van_id": 241, "first_name": "Erika", "last_name": "Testuser", "city": "Decatur", "state": "AL"}',), {'json':0}),
        bigquery.Row(('{"van_id": 110, "first_name": "Ulysses", "last_name": "Testuser", "city": "Hoover", "state": "AL"}',), {'json':0}),
        bigquery.Row(('{"van_id": 242, "first_name": "Earl", "last_name": "Testuser", "city": "Santa Clara", "state": "CA"}',), {'json':0}),
        bigquery.Row(('{"van_id": 111, "first_name": "Quinn", "last_name": "Testuser", "city": "Clovis", "state": "CA"}',), {'json':0}),
        bigquery.Row(('{"van_id": 103, "first_name": "Jane", "last_name": "Testuser", "city": "San Bernardino", "state": "CA"}',), {'json':0}),
        bigquery.Row(('{"van_id": 117, "first_name": "Sylvia", "last_name": "Testuser", "city": "Elk Grove", "state": "CA"}',), {'json':0})
    ]
    mock_query_job.result.return_value = mock_rows
    mock_bigquery.query.return_value = mock_query_job
    data = get_source_data(mock_bigquery, "dataset", "test_table")
    assert len(data) == 6
    assert data[0]['van_id'] == 241

# def test_write_data_to_file():
#     assert False

# def test_main():
#     # dataset doesn't exist
#     # dataset does exist
#     assert False
