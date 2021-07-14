import logging, sys # Import internal packages first.

import pandas as pd # Import custom modules second

# Reference internal methods last
from connectors.databases import mysql
from connectors.storage import azure
from helpers.functions import load_arguments, apply_filter, process_data, write_to_csv, preprocess_taxi_data, \
    preprocess_weather_data

#######################################
# Project Setup
#######################################
## Setup logger
logging.basicConfig(filename="app.log",
                        filemode="w+",
                        level=logging.ERROR,
                        format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Start")

## Load all arguments from script initiation
args = load_arguments()

#######################################
# Load Data
#######################################

## Taxi data from database
dfTripData = mysql.execute_read_query("SELECT * FROM cab_trip_data;") # I could have defined the needed columns upfront, but I prefer bring in all data at first
## Weather data from csv
dfWeather = pd.read_csv("weather.csv")

#######################################
# Preprocess data - Refer to data profiling report for data layout
#######################################
dfTripData = preprocess_taxi_data(dfTripData)
dfWeather = preprocess_weather_data(dfWeather)

#######################################
# Process data - Refer to data profiling report for data layout
#######################################
dfFilteredTripData = apply_filter(dfTripData,args)
dfResult = process_data(dfFilteredTripData, dfWeather,args)

#######################################
# Output Result to CSV
#######################################
write_to_csv(dfResult)

#######################################
# Upload file to Azure Blob Storage
#######################################
azure.upload_file()