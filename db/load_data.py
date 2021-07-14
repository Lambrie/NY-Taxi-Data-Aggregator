import os
from pathlib import Path

from connectors.databases.mysql import execute_insert_query

package_directory = Path(__file__).resolve().parent.parent
os.chdir(package_directory)

## Load data into database
try:
    with open("./db/ny_cab_trips_data.sql", "r+") as file:
        query = file.read()
        execute_insert_query(query)
except FileNotFoundError:
    print("File not found")
except Exception as e:
    print(e)
else:
    print("Script Completed")