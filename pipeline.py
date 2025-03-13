# pipeline.py
import logging
from scripts.data_fetch import fetch_data
from scripts.data_store import store_partitioned_data
from scripts.data_validation import validate_data
from scripts.data_cleaning import clean_data
from scripts.feature_engineering import create_features
from scripts.feature_store import store_features_in_sql, create_feature_metadata
from scripts.model_training import train_model

def main():
    logging.basicConfig(level=logging.INFO, filename='logs/pipeline.log')

    # Fetch data
    data = fetch_data()

    # Validate
    validate_data(data)

    # Clean
    cleaned_data = clean_data(data)

    # Store Raw
    store_partitioned_data(cleaned_data, 'data/raw', 'date')

    # Feature Engineering
    features = create_features(cleaned_data)

    # Store Processed
    store_partitioned_data(features, 'data/processed', 'date')

    # Store Features in SQL
    store_features_in_sql(features, 'data/features.db')

    # Train Model
    model = train_model(features.drop('target', axis=1), features['target'])

if __name__ == "__main__":
    main()