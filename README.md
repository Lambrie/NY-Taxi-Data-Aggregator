# NY Taxi Data Aggregator

This is a basic script that performs the following functions:
* Accepts input parameters for data filtering
* Connects to a mySQL database and extracts data from a single table for NY cab data
* Load a csv file for Weather data
* Transforms the mySQL and CSV data into a single aggregated view by date
* Exports the result to a pipe delimited CSV file
* Upload the generated csv file to Azure Blob storage

The script is dependent on 3 input parameters:
1. Start Date - Mandatory - yyyy-mm-dd
2. End Date - Mandatory - yyyy-mm-dd
3. Medallion - Optional - Must be a comma seperated list eg. xxx,xxx

The script can be executed in 2 ways:
* Local runtime; and
* Docker

### Local runtime setup

1. Run pip install -r requirements.txt
2. Configure the database_config.json file to point to your mySQL datasource
3. Execute the assessement.py script
```commandline
python assessment.py 2013-12-01 2013-12-31 D7D598CD99978BD012A87A76A7C891B7,5455D5FF2BD94D10B304A15D4B7F2735
```

### Docker Setup
1. To run docker, ensure you have docker-compose installed
2. Change directory to the root of the project folder where the docker-compose.yml file is located
3. Excute docker up command
```commandline
docker-compose up db
```
Wait a few minutes for the database to be created and initialized with the taxi data.
The create script locks the tables, so the app won't be able to access the database.
After a few minutes run the application
```commandline
docker-compose up app
```
If both containers have already been created before the following can command can be executed at any time
```commandline
docker-compose up
```

To change the arguments in the docker run, open the docker-compose.yml file and change the parameters as below
```yaml
command: python assessment.py 2013-12-01 2013-12-31
```

With both methods, an app.log file will be generated and a results.csv file will be generated if no errors occurred

If the AZURE_STORAGE_CONNECTION_STRING environment variable is set, the output file will be uploaded to Azure Blob storage

## Running Unit Test

In order to run the unit test, follow the follow step:
1. pip install -r requirements-test.txt
2. cd to project directory
3. Execute pytest
```commandline
pytest
```

13 Tests will be executed
```commandline
tests\test_functions.py .............                                                                            [100%]

======================================================= 13 passed in 0.71s ============================================

```

