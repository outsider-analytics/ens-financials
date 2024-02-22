import os
import yaml
from dune_client.client import DuneClient
from dotenv import load_dotenv
import sys
import codecs

# Set the default encoding to UTF-8
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)

dune = DuneClient.from_env()

# Read the queries.yml file
queries_yml = os.path.join(os.path.dirname(__file__), "..", "queries.yml")
with open(queries_yml, "r", encoding="utf-8") as file:
    data = yaml.safe_load(file)

# Extract the query_ids from the data
query_ids = [id for id in data["query_ids"]]

for id in query_ids:
    query = dune.get_query(id)
    print("PROCESSING: query {}, {}".format(query.base.query_id, query.base.name))

    # Check if query file exists in /queries folder
    queries_path = os.path.join(os.path.dirname(__file__), "..", "queries")
    files = os.listdir(queries_path)
    found_files = [
        file for file in files if str(id) == file.split("___")[-1].split(".")[0]
    ]

    if len(found_files) != 0:
        file_path = os.path.join(
            os.path.dirname(__file__), "..", "queries", found_files[0]
        )
        # Get the file name from the file path
        file_name = os.path.basename(file_path)
        # Separate the name of the file from the number after ___
        file_name_parts = file_name.split("___")
        query_name = file_name_parts[0]
        query_number = file_name_parts[1].split(".")[0]
        # Update the query name
        query.base.name = query_name

        # Read the content of the file
        with open(file_path, "r", encoding="utf-8") as file:
            text = file.read()
            print("file_name:", file_name)

            # Update existing file
            dune.update_query(
                query.base.query_id,
                query_sql=text,
                # added the below line to update the query name
                # todo: add case?
                name=query_name,
            )
            if query.base.name == query_name:
                print("SUCCESS: updated query {} to dune".format(query.base.query_id))
            else:
                print(
                    "SUCCESS: updated query {} to dune, changed name to {}".format(
                        query.base.query_id, query_name
                    )
                )
    else:
        print("ERROR: file not found, query id {}".format(query.base.query_id))
