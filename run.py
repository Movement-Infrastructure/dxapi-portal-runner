import os
import sys
import google.auth
from mig_dx_api import (
    DX,
    DatasetSchema,
    Installation,
    SchemaProperty
)
from mig_dx_api._dataset import DatasetOperations
from google.cloud import bigquery

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
        print(row.values()[0])
        data.append(row.values()[0])
    return data

def write_data_to_file(source_data: list, destination_dataset: DatasetOperations, upload_url: str):
    """
    Write data from Portal source dataset to file in MiG landing bucket
    """
    # Upload the data for the dataset in MIG to presigned url
    response = destination_dataset.upload_data_to_url(upload_url['url'], source_data)
    print(f'upload response: {response}')
    # Log results

def main():
    # Pass in name of BigQuery dataset from ScriptRunner
    dataset_id = sys.argv[1]
    table_name = sys.argv[2]
    # TODO: Error if params are missing
    print(f"dataset_id {dataset_id} and table_name {table_name}")

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
    # TODO: error if there are no installations
    # how do I know if this is the right installation?
    installation = installations[0]
    print(f'installation: {installation}')

    with dx.installation(installation) as ctx:
        destination_dataset = ctx.datasets.find(name=table_name)
        if not destination_dataset:
            print(f'Did not find dataset with name {table_name}. Creating...')
            # If dataset doesn't exist, get schema of source table and create MIG dataset
            schema = get_schema(client, table_name, dataset_id, project)
            destination_dataset = create_dataset(dx, installation, table_name, schema)
        else:
            print(f'Found dataset with name {table_name}. Updating...')

        # Get data from source dataset
        source_data = get_source_data(client, dataset_id, table_name)

        # Get signed uploadurl from MIG
        upload_url = destination_dataset.get_upload_url()

        # Write data to mig bucket for processing
        write_data_to_file(source_data, destination_dataset, upload_url)

if __name__ == '__main__':
    main()
