# scripts/data_validation.py
import pandas as pd
import logging
import numpy as np
from scipy import stats



def validate_data(data: pd.DataFrame):
    logging.info("\n%s", data.head())
    """Basic data validation checks."""

    continuous_col = ['tenure', 'MonthlyCharges', 'TotalCharges']
    categorical_col = ['gender', 'SeniorCitizen','Dependents', 'Partner', 'PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'StreamingTV', 'StreamingMovies', 'Contract', 'PaperlessBilling', 'PaymentMethod']

    """Null data"""
    ## log number of null values
    logging.info("\Check NULL data")
    for col in data.columns:
        logging.info(col+": %s", data[col].isnull().sum())
    

    """report inconsistent data types"""
    logging.info("\nCheck Data Types")
    logging.info(col+": %s", data.dtypes)
    
    
    """range of values"""
    logging.info("\nChecking Range of Values")
    for col in continuous_col:
        try:
            logging.info("Column Name: %s", col)
            logging.info("Max Value: %s", data[col].max())
            logging.info("Min Value: %s", data[col].min())
            logging.info("Min Value: %s", data[col].mean())
            ## print outliers
            logging.info("Outlier Values: %s", len(data[(np.abs(stats.zscore(data[col])) > 3)]))
    
        except Exception as e:
            logging.error(e)

            
    for col in categorical_col:
        try:
            logging.info("Column Name: %s", col)
            logging.info("Unique Values: %s", data[col].unique())
            logging.info("Distribution of Values: %s", data[col].value_counts())
        except Exception as e:
            logging.error(e)

    target_column = 'Churn'
    try:
        logging.info("Taget Column Name: %s", target_column)
        logging.info("Unique Values: %s", data[target_column].unique())
        logging.info("Distribution of Values: %s", data[target_column].value_counts())
    except Exception as e:
        logging.error(e)

    

    return data



