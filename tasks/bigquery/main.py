import os
import logging
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/airflow/credentials/credentials.json"


def test_bigquery():
    try:
        client = bigquery.Client()
        logging.info("BigQuery client has been initialized successfully!")
    except Exception as e:
        raise Exception(f"An error: {e}")


if __name__ == "__main__":
    test_bigquery()
