import os
import sys
from dune_client.client import DuneClient
from dotenv import load_dotenv


dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)

dune = DuneClient.from_env()

# Get ID for the query to download the CSV file
query_id = input("Enter the query ID: ")

# Download the CSV file
results_csv = dune.download_csv(query_id)

# Define the path for the CSV file in the 'downloads' directory, could change to query_id if better
csv_file_path = os.path.join(
    os.path.dirname(__file__), "..", "downloads", f"{query_id}_query_report.csv"
)

# Save the results to a CSV file
with open(csv_file_path, "wb") as f:
    f.write(results_csv.data.read())

print(f"CSV file has been saved to {csv_file_path}.")
