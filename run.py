import os
import sys
import google.auth
from mig_dx_api import DX
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

# If dataset doesn't exist, get schema of source table and create MIG dataset
def create_dataset():
    # TODO:
    # Get schema of source dataset
    # create Dataset
    return

# Write portal data to mig landing bucket
def write_data_to_file():
    # TODO:
    # Get data from source dataset
    # Get signed uploadurl from MIG
    # Upload the data for the dataset in MIG to presigned url
    # Log results
    return

# Check if dataset of specified name already exists
installations = dx.get_installations()
# TODO: error if there are no installations
# how do I know if this is the right installation?
installation = installations[0]

with dx.installation(installation) as ctx:
    dataset_ops = ctx.datasets.find(name=table_name)
    if not dataset_ops:
        print(f'Did not find dataset with name {table_name}. Creating...')
        create_dataset()
    else:
        print(f'Found dataset with name {table_name}. Updating...')

    # Write data to mig bucket to be processed
    write_data_to_file()
