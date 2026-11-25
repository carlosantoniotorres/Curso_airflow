from airflow.sdk import Asset

raw_dataset = Asset("file:///tmp/raw_data.json")
clean_dataset = Asset("file:///tmp/clean_data.json")
