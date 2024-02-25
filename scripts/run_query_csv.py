from io import BytesIO, StringIO
import os
import time
from dune_client.client import DuneClient
from dotenv import load_dotenv
import sys
import pandas as pd

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)

dune = DuneClient.from_env()

# get id passed in python script invoke
id = sys.argv[1]
print("id:", id)

# get the query object
query = dune.get_query(id)

res = dune.run_query_csv(query.base)

print("res: ", res)

# Convert CSV data to DataFrame
df = pd.read_csv(BytesIO(res.data.getvalue()))

query_name = query.base.name.replace(" ", "_") + ".csv"
results_folder = os.path.join(os.path.dirname(__file__), "..", "results")
csv_path = os.path.join(results_folder, query_name)

df.to_csv(csv_path, index=False)
print("CSV saved at:", csv_path)
# save the csv file
df.to_csv(csv_path, index=False)
print("CSV saved at:", csv_path)
