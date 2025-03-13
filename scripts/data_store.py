# scripts/data_store.py
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

def store_partitioned_data(data, path, partition_key, source):
    partition_key='date'
    path+=source
    """Store data in a partitioned format."""
    # Get current date
    logging.info("Starting Data Ingestion for source: "+str(source))
    current_date = datetime.now()

    # Convert to string (YYYY-MM-DD format)
    current_date_str = current_date.strftime('%Y-%m-%d')

    logging.info("Ingesting Data for: "+str(current_date_str))
    partition_path = os.path.join(path, f"{partition_key}={current_date_str}")
    os.makedirs(partition_path, exist_ok=True)
    
    data.to_csv(os.path.join(partition_path, f"{current_date_str}_data.csv"), index=False)

    logging.info("Completed Data Ingestion")