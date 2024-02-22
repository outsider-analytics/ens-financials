import os
import sys
from dune_client.client import DuneClient
from dotenv import load_dotenv
import pandas as pd

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)

dune = DuneClient.from_env()

queries_path = os.path.join(os.path.dirname(__file__), "..", "queries")
files = os.listdir(queries_path)
found_files = [file for file in files if "___" not in file and file.endswith(".sql")]

for file in found_files:
    print("Adding:", file)
    file_path = os.path.join(queries_path, file)

    # Extract the file name without the .sql extension
    file_name_without_extension = os.path.splitext(file)[0]

    # Read the content of the file
    with open(file_path, "r", encoding="utf-8") as file_content:
        text = file_content.read()
        print("file_name:", file_name_without_extension)

        # Create a new query with the correct file name
        res = dune.create_query(name=file_name_without_extension, query_sql=text)
        # Extract the query_id from the result
        query_id = res.base.query_id

        # Append the query_id to the file name
        new_file_name = f"{file_name_without_extension}___{query_id}.sql"

        # Append the query_id to the queries.yml file
        with open(
            "/home/outsider_analytics/Code/ens-financials/queries.yml", "a"
        ) as queries_file:
            queries_file.write(f"  - {query_id}\n")

        # Rename the file with the new file name
        new_file_path = os.path.join(queries_path, new_file_name)
        os.rename(file_path, new_file_path)
