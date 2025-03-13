import pandas as pd
import logging
import json
import os
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder

def create_feature_metadata():
    """Define metadata for each feature."""
    metadata = {
        "tenure_bucket": "Forming 3 buckets. 0-6 months, 6-12 months, 13-24 months and 24+ months. 0,1,2,3 respective values",
        "service_count": "Count active services (PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies",
        "average_charge": "TotalCharges / tenure"
    }
    logging.info("Metadata: %s", metadata)

    # Check if the file exists
    file_path = ".data/processed/metadata.json"
    print(os.getcwd())
    if not os.path.exists(os.path.join(os.getcwd(),file_path)):
        # Create an empty JSON structure
        with open(file_path, 'w') as f:
            json.dump({}, f)  # Empty JSON object

    with open(file_path, "r+") as f:
        f.write(json.dumps(metadata))

    return metadata

def create_features(data: pd.DataFrame):
    create_feature_metadata()
    """Create aggregated features."""
    data['tenure_years'] = data['tenure'] / 12
    
    # Create tenure buckets
    data['tenure_bucket'] = pd.cut(data['tenure'], 
                                bins=[0, 6, 12, 24, float('inf')],  # Define the bucket ranges
                                labels=[0, 1, 2, 3],                 # Assign corresponding values
                                right=True)                          # Include the right edge (e.g., 6 falls in [0-6])
    
    data["provided_services"] = data['PhoneService'] + data['MultipleLines'] + data['InternetService'] + data['OnlineSecurity'] + data['OnlineBackup'] + data['DeviceProtection'] + data['TechSupport'] + data['StreamingTV'] + data['StreamingMovies']

    data["average_charges"] = data["TotalCharges"]/data["tenure"]

    """Standardize numerical values"""
    continuous_col = ['tenure', 'MonthlyCharges', 'TotalCharges']
    categorical_col = ['gender', 'SeniorCitizen','Dependents', 'Partner', 'PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'StreamingTV', 'StreamingMovies', 'Contract', 'PaperlessBilling', 'PaymentMethod']

    scaler = StandardScaler()
    data[continuous_col] = scaler.fit_transform(data[continuous_col])
    logging.info("Standardized Numerical Columns")

    """convert categorical variables to numeric"""
    le = LabelEncoder()
    cols = [x for x in categorical_col if x != 'SeniorCitizen']
    for col in cols:
        data[col+'_embed'] = le.fit_transform(data[col])
    # logging.info("\n%s", list(data.iloc[0]))


    return data
