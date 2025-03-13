from sklearn.ensemble import RandomForestClassifier
import logging
import os

# Set up logging to a custom file
log_file_path = os.path.join("./logs/error_logs.log")
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def train_model(features, target):
    model = RandomForestClassifier()
    model.fit(features, target)
    return model