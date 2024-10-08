import argparse
import json
import os
import google.auth
from mig_dx_api import (
    DX,
    DatasetSchema,
    Installation,
    SchemaProperty
)
from mig_dx_api._dataset import DatasetOperations
from google.cloud import bigquery

def get_target_installation(installations: list[Installation], target_installation_id: str) -> Installation:
    if len(installations) == 0:
        raise Exception("No valid installations found")
    elif len(installations) == 1:
        target_install = installations[0]
        # error if the target installation id doesn't match the only existing installation
        if target_installation_id and target_install.movement_app_installation_id != int(target_installation_id):
            raise Exception(f"Installation {target_installation_id} not found")
        return target_install
    else:
        if target_installation_id is None:
            raise Exception("More than one installation available and no target installation id specified")
        target_install_id = int(target_installation_id)
        for install in installations:
            if install.movement_app_installation_id == target_install_id:
                target_install = install
                break
        if target_install is None:
            raise Exception("Installation {target_installation_id} not found")
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

    print(schema)
    print(f"table constraints: {primary_key}")

    properties = []
    for field in schema:
        # We don't care about the actual type of the field, the type is always "string"
        property = SchemaProperty(name=field.name, type="string", required=not field.is_nullable)
        properties.append(property)

    return DatasetSchema(
        properties=properties,
        primary_key=primary_key
    )

def create_dataset(dx: DX, installation: Installation, dataset_name: str, dataset_schema: DatasetSchema) -> DatasetOperations:
    """
    Create MiG dataset using mig-dx-api client
    """
    # create MiG dataset with source schema
    with dx.installation(installation) as ctx:
        new_dataset = ctx.datasets.create(
            name=dataset_name,
            description='Dataset created through Portal Script Runner',
            schema=dataset_schema
        )
        print(f'new dataset: {new_dataset}')
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
    data = []
    for row in rows:
        row_data_string = row.values()[0]
        data.append(json.loads(row_data_string))
    return data

def write_data_to_file(source_data: list, destination_dataset: DatasetOperations, upload_url: str):
    """
    Write data from Portal source dataset to file in MiG landing bucket
    """
    # Upload the data for the dataset in MIG to presigned url
    response = destination_dataset.upload_data_to_url(upload_url['url'], source_data)
    # Log results
    print(f'upload response: {response}')

def main(dataset_id: str, table_name: str, target_installation_id: str):
    print(f"Source dataset: {dataset_id}.{table_name}")

    # Initialize the mig client
    private_key = f"-----BEGIN PRIVATE KEY-----\n{os.environ.get("PRIVATE_KEY")}\n-----END PRIVATE KEY-----"
    dx = DX(app_id=os.environ.get("APP_ID"), private_key=private_key)
    dx.base_url = os.environ.get("BASE_URL")
    user_info = dx.whoami()
    print(f"user info: {user_info}\n")

    # Initialize the google bigquery client
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    print(f"bigquery project: {project}")
    client = bigquery.Client(credentials=credentials, project=project)

    # Check if dataset of specified name already exists
    installations = dx.get_installations()

    installation = get_target_installation(installations, target_installation_id)

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

        # Get signed uploadurl from MIG
        upload_url = destination_dataset.get_upload_url(mode='replace')

        # Write data to mig bucket for processing
        write_data_to_file(source_data, destination_dataset, upload_url)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_id', dest='dataset_id', type=str, help='Source dataset id', required=True)
    parser.add_argument('--table_name', dest='table_name', type=str,
        help='Source table name, also used to name destination dataset', required=True)
    parser.add_argument('--installation_id', dest='target_installation_id', type=str,
        help='Target installation id (optional when there is only one installation)', required=False)
    args = parser.parse_args()
    # Pass in name of BigQuery dataset from ScriptRunner
    main(args.dataset_id, args.table_name, args.target_installation_id)
