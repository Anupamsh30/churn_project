import logging
import pandas as pd
import sqlite3
import os

def fetch_data_source_1():
    ## create connection with sqlite database
    try:
        
        conn = sqlite3.connect('data_ingestion.db')
        df = pd.read_sql_query("SELECT * FROM customers", conn)
        return df
    except Exception as err:
        logging.error("Fetching data from sql source failed")
        raise err

def fetch_data_source_2():
    try:
        # Download latest version
        print(os.getcwd())
        data = pd.read_csv('./data/source/bank_customer_churn.csv')
        return data
    except Exception as err:
        logging.error("Fetching data from csv source failed")
        raise err


def fetch_data():
    """Automates fetching data from APIs or databases."""
    logging.info("Fetching data from sources...")
    
    logging.info("Fetching data from sql source...")
    data_1 = fetch_data_source_1()
    logging.info("Data Fetch Completed")
    logging.info("Number of rows in the data: "+str(len(data_1)))

    logging.info("Fetching data from csv source...")
    data_2 = fetch_data_source_2()
    logging.info("Data Fetch Completed")
    logging.info("Number of rows in the data: "+str(len(data_1)))

    frames = [data_1, data_2]
    df = pd.concat(frames)
    # Log the first 5 rows
    logging.info("\n%s", data_1.head())
    

    return frames
