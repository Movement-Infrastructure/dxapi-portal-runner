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

def get_schema() -> DatasetSchema:
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

def create_dataset(installation: Installation) -> DatasetOperations:
    """
    Create MiG dataset using mig-dx-api client
    """
    # Get schema of source dataset
    dataset_schema = get_schema()

    # create MiG dataset with source schema
    with dx.installation(installation) as ctx:
        dataset_name = f'{table_name}-phoenix'
        new_dataset = ctx.datasets.create(
            name=dataset_name,
            description='Dataset created through Portal Script Runner',
            schema=dataset_schema
        )
        print(f'new dataset: {new_dataset}')
        return new_dataset

def get_source_data() -> list:
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

def write_data_to_file(destination_dataset: DatasetOperations):
    """
    Write data from Portal source dataset to file in MiG landing bucket
    """
    # Get data from source dataset
    source_data = get_source_data()

    # Get signed uploadurl from MIG
    upload_url = destination_dataset.get_upload_url()

    # Upload the data for the dataset in MIG to presigned url
    response = dataset_ops.upload_data_to_url(upload_url['url'], source_data)
    print(f'upload response: {response}')
    # Log results

# Check if dataset of specified name already exists
installations = dx.get_installations()
# TODO: error if there are no installations
# how do I know if this is the right installation?
installation = installations[0]

with dx.installation(installation) as ctx:
    dataset_ops = ctx.datasets.find(name=table_name)
    if not dataset_ops:
        print(f'Did not find dataset with name {table_name}. Creating...')
        # If dataset doesn't exist, get schema of source table and create MIG dataset
        dataset_ops = create_dataset()
    else:
        print(f'Found dataset with name {table_name}. Updating...')

    # Write data to mig bucket for processing
    write_data_to_file(dataset_ops)
