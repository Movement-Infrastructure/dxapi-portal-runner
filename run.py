import argparse
import csv
import datetime
import io
import json
import os
import re
import requests
import google.auth
from google.cloud import (bigquery, secretmanager)
from google.cloud.secretmanager import SecretManagerServiceClient
from mig_dx_api import (
    DX,
    DatasetSchema,
    Installation,
    SchemaProperty
)
from mig_dx_api._dataset import DatasetOperations
from google.cloud import bigquery
CHUNK_SIZE = 1024 * 1024 * 16 # 16 MiB

def get_formatted_date() -> str:
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')

def get_target_installation(installations: list[Installation], workspace_id: str = None) -> Installation:
    if len(installations) == 0:
        raise Exception('No valid installations found')
    else:
        if workspace_id is None:
            raise Exception('No target workspace id specified')
        target_workspace_id = int(workspace_id)
        target_install = None
        for install in installations:
            if install.workspace_id == target_workspace_id:
                target_install = install
                break
        if target_install is None:
            raise Exception(f'Installation for workspace {workspace_id} not found')
        return target_install

def get_schema(client: bigquery.Client, table_name: str, dataset_id: str, project: str) -> DatasetSchema:
    """
    Get schema of source dataset from Portal BigQuery
    """
    dataset_ref = client.dataset(dataset_id=dataset_id, project=project)

    table_ref = dataset_ref.table(table_name)

    table = client.get_table(table_ref)

    schema = table.schema
    table_constraints = table.table_constraints # might not exist
    primary_key = table_constraints.primary_key.columns if table_constraints else []

    print(f'{get_formatted_date()} | bigquery schema: {schema}')
    print(f'table constraints: {primary_key}')

    properties = []
    for field in schema:
        # We don't care about the actual type of the field, the type is always "string"
        property = SchemaProperty(name=field.name, type='string', required=not field.is_nullable)
        properties.append(property)

    return DatasetSchema(
        properties=properties,
        primary_key=primary_key
    )

def create_dataset(dx: DX, installation: Installation, dataset_name: str, dataset_schema: DatasetSchema) -> DatasetOperations:
    """
    Create MIG dataset using mig-dx-api client
    """
    # create MIG dataset with source schema
    with dx.installation(installation) as ctx:
        new_dataset = ctx.datasets.create(
            name=dataset_name,
            description='Dataset created through Portal Script Runner',
            schema=dataset_schema
        )
        print(f'{get_formatted_date()} | new dataset: {new_dataset}')
        return new_dataset

def get_source_data(client: bigquery.Client, dataset_id: str, table_name: str) -> list:
    """
    Get data from Portal source dataset
    """
    sql = f"""
        SELECT TO_JSON_STRING(t) json
        FROM (
            SELECT *
            FROM `{dataset_id}.{table_name}`
        ) t
    """
    query_job = client.query(sql)
    rows = query_job.result()
    print(f'{get_formatted_date()} | fetched {rows.total_rows:,} rows from {dataset_id}.{table_name}')
    data = []
    for row in rows:
        row_data_string = row.values()[0]
        data.append(json.loads(row_data_string))
    print(f'{get_formatted_date()} | finished collecting data')
    return data

def create_data_buffer(source_data: list) -> io.StringIO:
    fieldnames = source_data[0].keys()
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(source_data)
    return buffer

def get_upload_url(dataset: DatasetOperations, is_resumable: bool = False):
    if is_resumable:
        return dataset.get_upload_url(mode='replace', upload_type='resumable')

    return dataset.get_upload_url(mode='replace')

def write_data_to_signed_url(source_data: list, destination_dataset: DatasetOperations, upload_url: str):
    # Upload the data for the dataset in MIG to presigned url
    response = destination_dataset.upload_data_to_url(upload_url['url'], source_data)
    # Log results
    print(f'upload response: {response}')

def write_chunked_data(
    data_buffer: io.StringIO,
    approx_data_size: int,
    upload_url: str,
    chunk_size: int):
    """
    Write data from Portal source dataset to file in MIG landing bucket
    """
    data_buffer.seek(0)

    # Track the start byte for each chunk
    start_byte = 0

    # Iterate over the buffered data by chunk
    while True:
        chunk = data_buffer.read(chunk_size)
        if not chunk:
            print('end of file, break')
            break  # Break if the end of file is reached

        end_byte = start_byte + len(chunk) - 1
        headers = {
            'Content-Range': f'bytes {start_byte}-{end_byte}/{approx_data_size}',
            'Content-Type': 'text/csv',
        }
        print(f'{get_formatted_date()} | attempting to send {start_byte} to {end_byte} bytes')

        # Upload the chunk
        response = requests.put(upload_url['url'], headers=headers, data=chunk)
        print(f'response: {response}')

        if response.status_code in [200, 201]:
            print('Upload complete.')
            break  # Successfully uploaded the whole file
        elif response.status_code == 308:
            # 308 indicates that the upload is incomplete and we can continue
            print(f'Uploaded bytes {start_byte} to {end_byte}')
            start_byte = end_byte + 1
        else:
            # Handle upload failure
            raise Exception(f'Upload failed: {response.text}')

def format_private_key(unformatted_key: str) -> str:
    """
    Portal removes all linebreaks from secrets, so add line breaks after header and before footer of private key
    """
    pattern = '^(-----BEGIN PRIVATE KEY-----)(.*)(-----END PRIVATE KEY-----)$'
    key_parts = re.match(pattern, unformatted_key)
    
    if key_parts is None:
        raise Exception('Private key is malformed')
    return f'{key_parts[1]}\n{key_parts[2]}\n{key_parts[3]}'

def get_secret(client: SecretManagerServiceClient, project: str, secret_name: str):
    name = client.secret_version_path(project, secret_name, 'latest')
    response = client.access_secret_version(request={'name': name})
    return response.payload.data.decode('UTF-8')

def main(dataset_id: str, table_name: str):
    print(f'Source dataset: {dataset_id}.{table_name}')

    target_workspace_id = os.environ.get('WORKSPACE_ID')
    if not target_workspace_id:
        raise Exception('Required environment variable WORKSPACE_ID not found')

    # Initialize the google bigquery client
    credentials, project = google.auth.default(
        scopes=[
            'https://www.googleapis.com/auth/bigquery',
        ]
    )
    print(f'bigquery project: {project}')
    client = bigquery.Client(credentials=credentials, project=project)

    # Initialize the mig client
    client = secretmanager.SecretManagerServiceClient()

    private_key_secret_name=os.environ.get('PRIVATE_KEY_SECRET_NAME')
    app_id_secret_name = os.environ.get('APP_ID_SECRET_NAME')
    if not private_key_secret_name or not app_id_secret_name:
        raise Exception('Required secret name environment variables not found')

    private_key = get_secret(client, project, private_key_secret_name)
    app_id = get_secret(client, project, app_id_secret_name)

    dx = DX(app_id=app_id, private_key=private_key)

    if os.environ.get('BASE_URL'):
        dx.base_url = os.environ.get('BASE_URL')

    user_info = dx.whoami()
    if user_info:
        print('Authenticated to DX API')

    # Check if dataset of specified name already exists
    installations = dx.get_installations()

    installation = get_target_installation(installations, target_workspace_id)

    print(f'target installation found: {installation}')

    with dx.installation(installation) as ctx:
        try:
            destination_dataset = ctx.datasets.find(name=table_name)
        except KeyError:
            print(f'Did not find dataset with name {table_name}. Creating...')
            # If dataset doesn't exist, get schema of source table and create MIG dataset
            schema = get_schema(client, table_name, dataset_id, project)
            destination_dataset = create_dataset(dx, installation, table_name, schema)

        print(f'Found dataset with name {table_name}. Updating...')

        # Get data from source dataset
        source_data = get_source_data(client, dataset_id, table_name)

        # Create buffer of data for writing to mig bucket (so size can be checked)
        data_buffer = create_data_buffer(source_data)

        # estimate size of file based on number of characters
        approx_data_size = data_buffer.tell()
        print(f'{get_formatted_date()}| data size: {approx_data_size} bytes')

        # get upload url and write data to MIG bucket
        if approx_data_size > CHUNK_SIZE:
            print(f'data size is larger than {CHUNK_SIZE} bytes so sending in chunks')
            resumable_url = get_upload_url(destination_dataset, True)
            write_chunked_data(data_buffer, approx_data_size, resumable_url, CHUNK_SIZE)
        else:
            print(f'data size is smaller than {CHUNK_SIZE} bytes so sending all at once')
            upload_url = get_upload_url(destination_dataset)
            write_data_to_signed_url(data_buffer, destination_dataset, upload_url)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_id', dest='dataset_id', type=str, help='Source dataset id', required=True)
    parser.add_argument('--table_name', dest='table_name', type=str,
        help='Source table name, also used to name destination dataset', required=True)
    args = parser.parse_args()
    # Pass in name of BigQuery dataset from ScriptRunner
    main(args.dataset_id, args.table_name)
