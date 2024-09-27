import os
import sys
import sys
from mig_dx_api import DX

private_key = f"-----BEGIN PRIVATE KEY-----\n{os.environ.get("PRIVATE_KEY")}\n-----END PRIVATE KEY-----"
# Initialize the client
dx = DX(app_id=os.environ.get("APP_ID"), private_key=private_key)
dx.base_url = os.environ.get("BASE_URL")

user_info = dx.whoami()
print(f"user info: {user_info}\n")

# Pass in name of BigQuery dataset from ScriptRunner
dataset_id = sys.argv[1]
table_name = sys.argv[2]

print(f"dataset_id {dataset_id} and table_name {table_name}")

# Check if dataset of specified name already exists
installations = dx.get_installations()
# error if there are no installations

# how do I know if this is the right installation?
installation = installations[0]

with dx.installation(installation) as ctx:
    dataset_ops = ctx.datasets.find(name=table_name)
    if dataset_ops:
        print(f'Found dataset with name {table_name}. Updating...')
        # Get data from source dataset
        # Get signed uploadurl from MIG
        # Upload the data for the dataset in MIG to presigned url
        # Log results
    else:
        print(f'Did not find dataset with name {table_name}. Creating...')

# If dataset doesn't exist, get schema of source table and create MIG dataset
def create_dataset():
    return