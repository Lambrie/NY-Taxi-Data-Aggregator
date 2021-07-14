import argparse, sys, logging
from datetime import datetime
import pandas as pd

def validate_date(dateValue,return_value=False):
    """
    Stand alone function to validate dates and formats. Becuase this output of this script is
    date driven there was a need for a reuasble data validation and formatting function

    :param dateValue: Date string needed for validation
    :param return_value: Trigger to determine if boolean value for validity should be return or valid result
    :return: Boolean / Valid Date Result as Date format
    """
    try:
        dt = datetime.strptime(str(dateValue), "%Y-%m-%d")
        d_value = datetime(year=dt.year, month=dt.month, day=dt.day).date()
    except:
        logging.error(f"Invalid date provided: {dateValue}. The date format should comply to yyyy-mm-dd")
        return None
    else:
        if return_value: return d_value
        else: return True

def load_arguments():
    """
    Argument parser, used to parse and validate arguments received by the script on execution
    :return: args Object
    """
    try:
        # Create the Argument parser
        my_parser = argparse.ArgumentParser()
        # Add the Arguments
        my_parser.add_argument('pickup_start_date',
                               type=str,
                               help='Pickup start date')
        my_parser.add_argument('pickup_end_date',
                               type=str,
                               help='Pickup end date')
        my_parser.add_argument('medallion',
                               nargs='*',
                               # action='append',
                               type=str)
        args = my_parser.parse_args()

        if len(args.medallion) > 1:
            print("Medalion argument should be seperated by commas and not spaces")
            logging.error(f"Medalion argument should be seperated by commas and not spaces")
            sys.exit()

        if validate_date(args.pickup_start_date) and validate_date(args.pickup_end_date):
            return args
        else:
            logging.error(f"Invalid date format supplied")
            print("Invalid date format supplied")
            sys.exit()

    except Exception as e:
        logging.error(f"Invalid arugments provided - {e}")
        sys.exit()

def preprocess_taxi_data(df):
    """
    Preprocessing taxi data

    This function only performs preprocessing of NY Taxi Data. It remove unwanted columns
    and format date columns into a useable date only format

    :param df: Pandas dataframe
    :return: df: Processed Pandas Dataframe
    """
    try:
        # Convert datetime columns to date
        df["pickup_date"] = pd.to_datetime(df.loc[:, ("pickup_datetime")]).dt.date
        df["dropoff_date"] = pd.to_datetime(df.loc[:, ("dropoff_datetime")]).dt.date  # https://www.statology.org/convert-datetime-to-date-pandas/
        # Drop nulls and columns not used for this assignment
        df = df.drop(["hack_license", "vendor_id", "rate_code", "store_and_fwd_flag",
                                      "pickup_longitude", "pickup_latitude", "dropoff_longitude",
                                      "dropoff_latitude"], axis=1)
    except AttributeError:
        logging.error("Taxi data did not load from source")
        sys.exit()
    else:
        return df

def preprocess_weather_data(df):
    """
        Preprocessing weather data

        This function only performs preprocessing of weather data. It remove unwanted columns
        and formats date columns into a useable date only format

        :param df: Pandas dataframe
        :return: df: Processed Pandas Dataframe
        """
    try:
        # Convert datetime columns to date, reformat and add to new column
        df["pickup_date"] = pd.to_datetime(df.loc[:, ("Date time")], format="%m/%d/%Y").dt.date
        # Drop unused columns
        df = df.drop(["Date time", "Conditions"], axis=1)
    except AttributeError:
        logging.error("Weather data did not load from source")
        sys.exit()
    else:
        return df

def apply_filter(df, args):
    """
    This function applies the arguments to the preprocessed dataframe containing taxi data

    :param df: Preprocessed Pandas dataframe
    :param args: Arguments object
    :return: df: Filtered Pandas dataframe
    """
    logging.info(f"""Parameters used: 
                         Medallion: {args.medallion}
                         Start Date: {",".join(args.pickup_start_date)}
                         End Date: {",".join(args.pickup_end_date)}
                      """)
    if len(args.medallion)==1:
        # If list of medallions provided filter out first before applying group by
        if "," in args.medallion[0]:
            # Seperate the string value into a list by ','
            medallionList = [str(item) for item in args.medallion[0].split(',')]
        else:
            # If medallions parameter does not contain a comma it represents a single object
            medallionList = args.medallion
        df = df[(df["medallion"].isin(medallionList))]
    elif len(args.medallion) > 1:
        # If more than 1 value appears in the list the arguments were passed incorrect, ensure all medallions are
        # seperated by a comma and not a space
        print("Invalid medallion list supplied")
        logging.error("Invalid medallion list supplied")
        sys.exit()

    # Convert string date from args to date format
    start_date = validate_date(args.pickup_start_date, return_value=True)
    end_date = validate_date(args.pickup_end_date, return_value=True)
    # Filter data based on start and end dates provided
    return df[(df['pickup_date'] >= start_date) & (df['pickup_date'] <= end_date)]


def process_data(dfTripData,dfWeather, args):
    """
    Once all data has been preprocessed and filtered according to the arguments. The dat will be aggregated into the
    results view.

    Data is processed by a group by clause which groups all taxi data by date and performs a sum function over
    the passenger count, total trip time and trip distance.

    Once the taxi data has been aggregated by date, the weather information containing the temperature is inner joined
    on the date value.

    :param dfTripData: pandas dataframe
    :param dfWeather: pandas dataframe
    :param args: Arguments object
    :return: dfTripWeatherDataGrouped: pandas dataframe
    """
    if not dfTripData.empty:
        # Apply group by function on filtered data and sum on 3 fields ["passenger_count","trip_time_in_secs","trip_distance"]
        # https://realpython.com/pandas-groupby/
        dfTripDataGrouped = dfTripData.groupby("pickup_date", as_index=False)["passenger_count", "trip_time_in_secs",
                                                                              "trip_distance"].sum()
        # Merge trip data with weather information
        dfTripWeatherDataGrouped = pd.merge(dfTripDataGrouped, dfWeather, on='pickup_date', how='inner')
        # Rename columns to user friendly well formatted column names
        dfTripWeatherDataGrouped.rename(columns={'pickup_date': 'Pickup Date',
                                                 'passenger_count': 'Passengers',
                                                 'trip_time_in_secs': 'Travel Time(s)',
                                                 'trip_distance': 'Travel Distance(Km)',
                                                 'Temperature': 'Temperature(F)'},
                                        inplace=True)
        return dfTripWeatherDataGrouped
    else:
        logging.info(f"No data available between dates {args.pickup_start_date} and {args.pickup_end_date}")
        print("No data found for parameters")
        return pd.DataFrame()

def write_to_csv(df):
    """
    Generate a csv file of the results obtained from the above function.

    :param df: pandas dataframe
    """
    if not df.empty:
        # Write result to pipe delimited csv
        df.to_csv("result.csv", sep='|', encoding='utf-8', index=False)
        logging.info(f"File result.csv created")
        print("File generated")