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

res = dune.run_query(query.base)

print("\n")
print(res)
