import os

# Define directory structure
dirs = [
    "data/raw",
    "data/processed",
    "logs",
    "scripts"
]

files = [
    "scripts/__init__.py",
    "scripts/data_fetch.py",
    "scripts/data_store.py",
    "scripts/data_validation.py",
    "scripts/data_cleaning.py",
    "scripts/feature_engineering.py",
    "scripts/model_training.py",
    "scripts/feature_store.py",
    "airflow_dag.py",
    "pipeline.py",
    "README.md"
]

# Create directories
for dir_path in dirs:
    os.makedirs(dir_path, exist_ok=True)

# Create empty files
for file_path in files:
    with open(file_path, 'w') as f:
        pass

print("Directory structure created successfully!")
