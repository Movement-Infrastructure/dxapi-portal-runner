import pytest
import datetime
from google.cloud import bigquery
from mig_dx_api import CreatedBy, Installation
from mig_dx_api._dataset import DatasetOperations
import requests
from uuid import uuid4
from unittest import mock
from unittest.mock import patch, MagicMock

from run import (
    get_target_installation,
    get_schema,
    get_source_data,
    create_data_buffer,
    get_upload_url,
    write_chunked_data,
    format_private_key
)

TEST_APP_ID = str(uuid4())
TEST_PRIVATE_KEY = 'private_key_contents'
TEST_CREATOR = CreatedBy(
    user_id = 123,
    display_name = 'Test User',
    email = 'test@example.com')

MOCK_RESPONSE_308 = MagicMock()
MOCK_RESPONSE_308.status_code = 308
MOCK_RESPONSE_200 = MagicMock()
MOCK_RESPONSE_200.status_code = 200
MOCK_RESPONSE_ERROR = MagicMock()
MOCK_RESPONSE_ERROR.status_code = 500
MOCK_RESPONSE_ERROR.text = 'Chunk failure'

def test_get_target_installation_default():
    workspace_id = '100'
    installations = [
        Installation(
            movement_app_id = str(uuid4()),
            installation_id = 1,
            workspace_id = int(workspace_id),
            created_by = TEST_CREATOR,
            date_created = datetime.datetime.now(),
            name = 'Test Installation'
        )
    ]

    target_installation = get_target_installation(installations, workspace_id)
    
    assert target_installation == installations[0]

def test_get_target_installation_with_install_id_argument():
    workspace_id = '31'

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
            installation_id = 35,
            workspace_id = int(workspace_id),
            created_by = TEST_CREATOR,
            date_created = datetime.datetime.now(),
            name = 'Test Installation 2'
        )
    ]
    target_installation = get_target_installation(installations, '31')
    
    assert target_installation == installations[1]

def test_get_target_installation_no_installations():
    with pytest.raises(Exception) as exception_info:
        installations = []
        get_target_installation(installations)
    assert str(exception_info.value) == 'No valid installations found'

def test_get_target_installation_with_installation_id_not_found():
    target_workspace_id = '21'
    with pytest.raises(Exception) as exception_info:
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
                workspace_id = 101,
                created_by = TEST_CREATOR,
                date_created = datetime.datetime.now(),
                name = 'Test Installation 2'
            )
        ]
        get_target_installation(installations, target_workspace_id)
    assert str(exception_info.value) == f'Installation for workspace {target_workspace_id} not found'

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
    assert schema.properties[0].type == 'string'

@patch('google.cloud.bigquery.Client', autospec=True)
def test_get_source_data(mock_bigquery):
    mock_query_job = mock.create_autospec(bigquery.QueryJob)
    # row data coming back from bigquery is a string, not json
    mock_rows = mock.create_autospec(bigquery.table.RowIterator)
    mock_rows.total_rows = 6

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
    data = get_source_data(mock_bigquery, 'dataset', 'test_table')
    assert len(data) == 6
    assert data[0]['van_id'] == 241

def test_create_data_buffer():
    source_data = [
        {'van_id': 241, 'first_name': 'Erika', 'last_name': 'Testuser', 'city': 'Decatur', 'state': 'AL'},
        {'van_id': 110, 'first_name': 'Ulysses', 'last_name': 'Testuser', 'city': 'Hoover', 'state': 'AL'},
        {'van_id': 242, 'first_name': 'Earl', 'last_name': 'Testuser', 'city': 'Santa Clara', 'state': 'CA'},
        {'van_id': 111, 'first_name': 'Quinn', 'last_name': 'Testuser', 'city': 'Clovis', 'state': 'CA'},
        {'van_id': 103, 'first_name': 'Jane', 'last_name': 'Testuser', 'city': 'San Bernardino', 'state': 'CA'},
        {'van_id': 117, 'first_name': 'Sylvia', 'last_name': 'Testuser', 'city': 'Elk Grove', 'state': 'CA'}
    ]
    data_buffer = create_data_buffer(source_data)

    approx_data_size_with_tell = data_buffer.tell()
    approx_data_size_with_len = len(data_buffer.getvalue())
    assert approx_data_size_with_tell == 238
    assert approx_data_size_with_len == 238

def test_get_resumable_upload_url():
    datasetOps = DatasetOperations(MagicMock(), MagicMock())
    datasetOps.get_upload_url = MagicMock()

    get_upload_url(datasetOps, True)

    # Check if the method was called with the correct arguments
    datasetOps.get_upload_url.assert_called_with(mode='replace', upload_type='resumable')

def test_get_signed_upload_url():
    datasetOps = DatasetOperations(MagicMock(), MagicMock())
    datasetOps.get_upload_url = MagicMock()

    get_upload_url(datasetOps)

    # Check if the method was called with the correct arguments
    datasetOps.get_upload_url.assert_called_with(mode='replace')

@patch('requests.put', autospec=True, side_effect=[MOCK_RESPONSE_308, MOCK_RESPONSE_308, MOCK_RESPONSE_200])
def test_write_chunked_data(mock_put):
    # size of this data is 238, so should be sent in three chunks
    source_data = [
        {'van_id': 241, 'first_name': 'Erika', 'last_name': 'Testuser', 'city': 'Decatur', 'state': 'AL'},
        {'van_id': 110, 'first_name': 'Ulysses', 'last_name': 'Testuser', 'city': 'Hoover', 'state': 'AL'},
        {'van_id': 242, 'first_name': 'Earl', 'last_name': 'Testuser', 'city': 'Santa Clara', 'state': 'CA'},
        {'van_id': 111, 'first_name': 'Quinn', 'last_name': 'Testuser', 'city': 'Clovis', 'state': 'CA'},
        {'van_id': 103, 'first_name': 'Jane', 'last_name': 'Testuser', 'city': 'San Bernardino', 'state': 'CA'},
        {'van_id': 117, 'first_name': 'Sylvia', 'last_name': 'Testuser', 'city': 'Elk Grove', 'state': 'CA'}
    ]
    data_buffer = create_data_buffer(source_data)
    data_size = data_buffer.tell()
    chunk_size = 100
    upload_url = {'url' : 'https://example.com'}

    write_chunked_data(data_buffer, data_size, upload_url, chunk_size)
    
    # check that requests put is called three times
    assert mock_put.call_count == 3

@patch('requests.put', autospec=True, side_effect=[MOCK_RESPONSE_ERROR])
def test_write_chunked_data_throws_error(mock_put):
    with pytest.raises(Exception) as exception_info:
        # size of this data is 238, so should be sent in three chunks
        source_data = [
            {'van_id': 241, 'first_name': 'Erika', 'last_name': 'Testuser', 'city': 'Decatur', 'state': 'AL'},
            {'van_id': 110, 'first_name': 'Ulysses', 'last_name': 'Testuser', 'city': 'Hoover', 'state': 'AL'},
            {'van_id': 242, 'first_name': 'Earl', 'last_name': 'Testuser', 'city': 'Santa Clara', 'state': 'CA'},
            {'van_id': 111, 'first_name': 'Quinn', 'last_name': 'Testuser', 'city': 'Clovis', 'state': 'CA'},
            {'van_id': 103, 'first_name': 'Jane', 'last_name': 'Testuser', 'city': 'San Bernardino', 'state': 'CA'},
            {'van_id': 117, 'first_name': 'Sylvia', 'last_name': 'Testuser', 'city': 'Elk Grove', 'state': 'CA'}
        ]
        data_buffer = create_data_buffer(source_data)
        data_size = data_buffer.tell()
        chunk_size = 100
        upload_url = {'url' : 'https://example.com'}

        write_chunked_data(data_buffer, data_size, upload_url, chunk_size)

    # check that exception is thrown
    assert str(exception_info.value) == f'Upload failed: {MOCK_RESPONSE_ERROR.text}'

def test_format_private_key():
    unformatted_key = '-----BEGIN PRIVATE KEY-----MIICUTCCAfugAwIBAgIBADANBgkqhkiG9w0BAQQFADBXMQswCQYDVQQGEwJDTjELMAkGA1UECBMCUE4xCzAJBgNVBAcTAkNOMQswCQYDVQQKEwJPTjELMAkGA1UECxMCVU4xFDASBgNVBAMTC0hlcm9uZyBZYW5nMB4XDTA1MDcxNTIxMTk0N1oXDTA1MDgxNDIxMTk0N1owVzELMAkGA1UEBhMCQ04xCzAJBgNVBAgTAlBOMQswCQYDVQQHEwJDTjELMAkGA1UEChMCT04xCzAJBgNVBAsTAlVOMRQwEgYDVQQDEwtIZXJvbmcgWWFuZzBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQCp5hnG7ogBhtlynpOS21cBewKE/B7jV14qeyslnr26xZUsSVko36ZnhiaO/zbMOoRcKK9vEcgMtcLFuQTWDl3RAgMBAAGjgbEwga4wHQYDVR0OBBYEFFXI70krXeQDxZgbaCQoR4jUDncEMH8GA1UdIwR4MHaAFFXI70krXeQDxZgbaCQoR4jUDncEoVukWTBXMQswCQYDVQQGEwJDTjELMAkGA1UEBQADQQA/ugzBrjjK9jcWnDVfGHlk3icNRq0oV7Ri32z/+HQX67aRfgZu7KWdI+JuWm7DCfrPNGVwFWUQOmsPue9rZBgO-----END PRIVATE KEY-----'
    formatted_key = format_private_key(unformatted_key)

    assert formatted_key == '-----BEGIN PRIVATE KEY-----\nMIICUTCCAfugAwIBAgIBADANBgkqhkiG9w0BAQQFADBXMQswCQYDVQQGEwJDTjELMAkGA1UECBMCUE4xCzAJBgNVBAcTAkNOMQswCQYDVQQKEwJPTjELMAkGA1UECxMCVU4xFDASBgNVBAMTC0hlcm9uZyBZYW5nMB4XDTA1MDcxNTIxMTk0N1oXDTA1MDgxNDIxMTk0N1owVzELMAkGA1UEBhMCQ04xCzAJBgNVBAgTAlBOMQswCQYDVQQHEwJDTjELMAkGA1UEChMCT04xCzAJBgNVBAsTAlVOMRQwEgYDVQQDEwtIZXJvbmcgWWFuZzBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQCp5hnG7ogBhtlynpOS21cBewKE/B7jV14qeyslnr26xZUsSVko36ZnhiaO/zbMOoRcKK9vEcgMtcLFuQTWDl3RAgMBAAGjgbEwga4wHQYDVR0OBBYEFFXI70krXeQDxZgbaCQoR4jUDncEMH8GA1UdIwR4MHaAFFXI70krXeQDxZgbaCQoR4jUDncEoVukWTBXMQswCQYDVQQGEwJDTjELMAkGA1UEBQADQQA/ugzBrjjK9jcWnDVfGHlk3icNRq0oV7Ri32z/+HQX67aRfgZu7KWdI+JuWm7DCfrPNGVwFWUQOmsPue9rZBgO\n-----END PRIVATE KEY-----'

def test_format_private_key_error():
    unformatted_key = '-----BEGIN CERTIFICATE-----MIICUTCCAfugAwIBAgIBADANDncEQX67aRfgZu7KWdI+JuWm7DCfrPNGVwFWUQOmsPue9rZBgO-----END CERTIFICATE-----'
    with pytest.raises(Exception) as exception_info:
        format_private_key(unformatted_key)
        assert str(exception_info.value) == 'Private key is malformed'
