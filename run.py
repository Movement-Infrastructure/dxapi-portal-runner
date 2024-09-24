import os
from mig_dx_api import DX

private_key = f"-----BEGIN PRIVATE KEY-----\n{os.environ.get("PRIVATE_KEY")}\n-----END PRIVATE KEY-----"
# Initialize the client
dx = DX(app_id=os.environ.get("APP_ID"), private_key=private_key)
dx.base_url = os.environ.get("BASE_URL")

user_info = dx.whoami()
print(f"user info: {user_info}\n")
