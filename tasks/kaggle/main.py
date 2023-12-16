import os
import logging
from pymongo import MongoClient


def download_csv(kaggle_credentials, mongo_credentials, **kwargs):
    print(kaggle_credentials["kaggle_user"])
    print(kaggle_credentials["kaggle_password"])
    print(mongo_credentials)
    dataset = kwargs["ti"]
    dataset = dataset.xcom_pull(task_ids="get_dataset_to_download", key="dataset")
    print(dataset)
