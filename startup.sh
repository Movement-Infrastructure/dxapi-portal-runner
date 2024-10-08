# Paste this code into the `startup command` text input box in portal
pip install -r requirements.txt
python src/dxapi-python-client/main.py --dataset_id $dataset_id --table_name $table_name
