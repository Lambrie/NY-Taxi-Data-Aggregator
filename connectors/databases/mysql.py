import logging
import pandas as pd
from .base import db_connector

import mysql.connector

@db_connector
def execute_insert_query(cnn,query):
    """
    This function is only used for insert and creation purposes and will not return any result

    :param cnn: Decorator
    :param query: Get query for insert/create statements
    """
    cur = cnn.cursor()
    try:
        cur.execute(query)
    except mysql.connector.Error as e:
        logging.error(e)
        raise
    except Exception as e:
        logging.error(e)
        cur.rollback()
    else:
        cur.commit()

@db_connector
def execute_read_query(cnn, query):
    """
    Function to read data from a mySql database and return the data as a pandas dataframe
    :param cnn:
    :param query: SQL query string to obtain data from database
    :return df: Dataframe returned from DB
    """
    try:
        df = pd.read_sql_query(query, cnn)
    except Exception as e:
        logging.error(e)
        return pd.DataFrame()
    else:
        return df