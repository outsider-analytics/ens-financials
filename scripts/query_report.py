import os
import time
import yaml
import pandas as pd
import requests
from dotenv import load_dotenv
from dune_client.client import DuneClient
from datetime import datetime
import subprocess

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)

dune = DuneClient.from_env()

# project name should change with the script
project_name = "lido"

# Define the path for the CSV file in the 'uploads' directory
csv_file_path = os.path.join(
    os.path.dirname(__file__), "..", "uploads", f"{project_name}_query_report.csv"
)

# Initialize an empty DataFrame with the desired columns
df = pd.DataFrame(
    columns=[
        "name",
        "query_Id",
        "execution_id",
        "execution_state",
        "submitted_at",
        "started_at",
        "completed_at",
        "execution_time",
        "pending_time",
        "rows",
        "report_date",
    ]
)

# Read the queries.yml file
with open("queries.yml", "r") as file:
    queries = yaml.safe_load(file)

# Convert each ID to string if needed
ids = [str(id) for id in queries["query_ids"]]

for id in ids:
    url = f"https://api.dune.com/api/v1/query/{id}/results"
    headers = {"X-DUNE-API-KEY": os.getenv("DUNE_API_KEY")}
    params = {"limit": 1, "offset": 0}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        # todo: This is purely for the name, we should use something different
        query = dune.get_query(id)

        response_data = response.json()

        # Prepare data for the DataFrame
        data = {
            "name": [query.base.name],
            "query_Id": [id],
            "execution_id": [response_data.get("execution_id")],
            "execution_state": [response_data.get("state")],
            "submitted_at": [response_data.get("submitted_at")],
            "started_at": [response_data.get("execution_started_at")],
            "completed_at": [response_data.get("execution_ended_at")],
            "execution_time": [
                response_data.get("result", {})
                .get("metadata", {})
                .get("execution_time_millis", 0)
                / 1000
            ],
            "pending_time": [
                response_data.get("result", {})
                .get("metadata", {})
                .get("pending_time_millis", 0)
                / 1000
            ],
            "rows": [
                response_data.get("result", {})
                .get("metadata", {})
                .get("total_row_count")
            ],
            "report_date": [datetime.utcnow().isoformat(timespec="microseconds") + "Z"],
        }

        # Append the current query's result to the DataFrame
        df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
        print(f"Data for query ID {id} has been fetched.")
    else:
        print(f"Error fetching data for query ID {id}: {response.status_code}")

# Write the DataFrame to the CSV file, overwriting any existing file
df.to_csv(csv_file_path, index=False)

print("CSV file has been updated.")

# Define the command to run the script
command = ["python", "scripts/upload_to_dune.py"]

# Execute the command
subprocess.run(command)
