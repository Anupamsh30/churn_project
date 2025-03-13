import pandas as pd
import logging
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

"""
Column wise observations from validation step:
1. No null data
2. Total Charges Column is object. It probably has a string outlier
3. Outlier value in total charges: ERROR - Could not convert string '29.851889.5108.151840.75151.65820.51949.4301.93046.053487.95587.45326.85681.15036.32686.057895.151022.957382.25528.3518
4. Unique Values:
    a. SeniorCitizen: [0,1]
    b. Dependents: ['Yes', 'No']
    c. Gender: ['Male', 'Female']
    d. Replace 'No phone service' with 'No' in columns, MultipleLines, OnlineSecurity, OnlineBackup, DeviceProtection, StreamingTV, StreamingMovies, 
"""


def clean_data(data: pd.DataFrame):
    """Clean and preprocess data."""
    data.drop_duplicates(inplace=True)

    """ drop duplicate index"""
    data.drop_duplicates(subset = ['customerID'], inplace = True)

    logging.info("Dropped Duplicates")

    """ fill null in all columns"""
    # Identify numerical and categorical columns
    data[['SeniorCitizen']] = data[['SeniorCitizen']].apply(lambda col: col.fillna(col.mode()[0]))
    continuous_col = ['tenure', 'MonthlyCharges', 'TotalCharges']
    categorical_col = ['gender', 'SeniorCitizen','Dependents', 'Partner', 'PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'StreamingTV', 'StreamingMovies', 'Contract', 'PaperlessBilling', 'PaymentMethod']

    logging.info("Replaced Nulls")

    """Fill missing values"""
    ## convert numerical datatype to numerical.
    data[continuous_col] = data[continuous_col].apply(lambda col: pd.to_numeric(col, errors='coerce'))

    ## filling continuos value with mean
    data[continuous_col] = data[continuous_col].apply(lambda col: col.fillna(col.mean()))

    ## filling categorical value with mean
    data[categorical_col] = data[categorical_col].apply(lambda col: col.fillna(col.mode()[0]))

    """Preprocessing string columns"""
    ## convert all string columns to lowercase
    string_col = [x for x in categorical_col if x != 'SeniorCitizen']
    data[string_col] = data[string_col].apply(lambda col: col.str.lower())
    ## remove extra spaces 
    data[string_col] = data[string_col].apply(lambda col: col.str.strip())
    ## converting "No Internet Service string to No"
    cols = ['MultipleLines', 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'StreamingTV', 'StreamingMovies']
    data[cols] = data[cols].replace('no phone service', 'no', regex=True)

    logging.info("Processed String Columns")

    ## convert numerical columns to numerical type
    # """printing correlation matrix"""
    # logging.info("Distribution of Categorical Values: %s", data[categorical_col].corr())

    
    
    # logging.info("Distribution of Values: %s", data[continuous_col].corr())
    # logging.info("Distribution of Continuous Values: %s", data[continuous_col].corr())
    """replacing the string in TotalCharges column to get rid of string values"""
    data[continuous_col] = data[continuous_col].apply(lambda col: pd.to_numeric(col, errors='coerce'))
    data.dropna(subset=continuous_col, inplace=True)

    

    """get correlation matrix"""
    ## add in end
    # logging.info("Correlation of Values: %s", data_encoded[continuous_col+categorical_col].corr())
    
    logging.info("Plotting Graphs: ")

    logging.info(f"\nShape: {data.shape}\n")
    # 2c
    print("Dataset Insights via Pair Plots which show relationship between all columns")
    # sns.pairplot(data)
    # Create distribution plots for all numeric columns
    output_dir = './data/plots/'
    for col in data.select_dtypes(include='number').columns:
        # Save the plot
        plot_path = f"{output_dir}/{col}_distribution.png"
        logging.info(plot_path)
        plt.figure(figsize=(8, 4))
        sns.histplot(data[col], kde=True, bins=30)
        plt.title(f'Distribution of {col}')
        
        
        plt.savefig(plot_path)
        plt.close()

        print(f"Saved: {plot_path}")
    # logging.info(plt.show())
    plt.figure(figsize=(8, 4))
    sns.pairplot(data)
    plt.savefig(plot_path)

    return data

