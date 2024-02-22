import os
import re
from dune_client.client import DuneClient
from dotenv import load_dotenv
import pandas as pd
from typing import Optional, List


dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)

dune = DuneClient.from_env()

# Ensure the queries_path dynamically adjusts to your project structure
queries_path = os.path.join(os.path.dirname(__file__), "..", "queries")
files = os.listdir(queries_path)
found_files = [file for file in files if "___" not in file and file.endswith(".sql")]

# Ensure queries.yml path is correctly set relative to this script's location
queries_yml_path = os.path.join(os.path.dirname(__file__), "..", "queries.yml")


def find_parameters_in_sql(sql_text: str):
    # Regex to find {{variable}} patterns, now including broader character set
    pattern = re.compile(r"\{\{([^\{\}]+)\}\}")
    matches = set(pattern.findall(sql_text))  # Use a set to ensure uniqueness
    # Assuming Dune's API expects parameters to be dictionaries with specific keys
    parameters = [
        {"name": match.strip(), "type": "text", "value": ""} for match in matches
    ]
    return parameters


for file in found_files:
    print("Adding:", file)
    file_path = os.path.join(queries_path, file)
    file_name_without_extension = os.path.splitext(file)[0]

    with open(file_path, "r", encoding="utf-8") as file_content:
        text = file_content.read()

        # Search for parameters in the SQL text
        parameters = find_parameters_in_sql(text)

        # Create a new query with the correct file name and parameters
        try:
            res = dune.create_query(
                name=file_name_without_extension, query_sql=text, params=parameters
            )
            query_id = res.base.query_id  # Extract the query_id from the result

            # Append the query_id to queries.yml
            with open(queries_yml_path, "a") as queries_file:
                queries_file.write(f"  - {query_id}\n")

            # Rename the file with the new file name
            new_file_name = f"{file_name_without_extension}___{query_id}.sql"
            new_file_path = os.path.join(queries_path, new_file_name)
            os.rename(file_path, new_file_path)
        except KeyError as e:
            print(f"Error extracting query_id from response: {e}")
        except Exception as e:
            print(f"Error creating query or updating files: {e}")
