import os
from mig_dx_api import DX

# Initialize the client
dx = DX(app_id=os.environ.get("APP_ID"), private_key=os.environ.get("PRIVATE_KEY"))
dx.base_url = os.environ.get("BASE_URL")

user_info = dx.whoami()
print(f"user info: {user_info}\n")