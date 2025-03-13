import sqlite3
import pandas as pd
import json
import logging

def store_features_in_sql(data, db_path):
    conn = sqlite3.connect(db_path)
    data.to_sql('features', conn, if_exists='replace', index=False)
    conn.close()


# def create_feature_metadata():
#     """Define metadata for each feature."""
#     metadata = {
#         "tenure_bucket": "Forming 3 buckets. 0-6 months, 6-12 months, 13-24 months and 24+ months. 0,1,2,3 respective values",
#         "service_count": "Count active services (PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies",
#         "average_charge": "TotalCharges / tenure"
#     }
#     logging.info("Metadata: %s", metadata)

#     with open("./metadata.json", "w") as f:
#         f.write(json.dumps(metadata))

    # return metadata
