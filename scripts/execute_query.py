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

res = dune.execute_query(query.base)
restype = type(res)
print("restype:", restype)
print("resState.state:", res.state)

count = 0
while str(res.state) in ["ExecutionState.PENDING", "ExecutionState.EXECUTING"]:
    time.sleep(5)
    count = count + 5
    res = dune.get_execution_status(res.execution_id)
    print("res after {} seconds:".format(count), res)

if "ExecutionState.FAILED" in str(res.state):
    print("Execution Failed")
    print("error: ", res)
else:
    print("Execution Successful!")
    print("result: ", res)
