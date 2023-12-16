import os
import logging
from pymongo import MongoClient

def download_csv(kaggle_credentials, mongo_credentials):
    os.environ["KAGGLE_USERNAME"] = kaggle_credentials["kaggle_user"]
    os.environ["KAGGLE_KEY"] = kaggle_credentials["kaggle_password"]
    from kaggle.api.kaggle_api_extended import KaggleApi

    try:
        api = KaggleApi()
        api.authenticate()
    except Exception as e:
        raise Exception(f"An error: {e}")

    try:
        connection_config = f"mongodb://{mongo_credentials['user']}:{mongo_credentials['password']}@{mongo_credentials['host']}:{mongo_credentials['port']}/"
        client = MongoClient(connection_config)
        db_connection = client[mongo_credentials["schema"]]
        collection = db_connection["initialize_airflow"]
        for i in collection.find({}):
            print(f"Estou aqui: {i}")
        print(f"It works: {db_connection}")
    except Exception as e:
        raise Exception(f"An error: {e}")
