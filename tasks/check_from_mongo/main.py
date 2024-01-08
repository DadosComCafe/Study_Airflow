from tasks.utils.mongo_wrapper import Mongo
import logging


def get_dataset_to_download(mongodb_credentials: dict, **kwargs):
    """Checks if there are any kaggle dataset records that have not yet been downloaded"""
    try:
        mongo_obj = Mongo(credentials=mongodb_credentials)
        myclient = mongo_obj.get_client()
        mydb = myclient[mongodb_credentials["schema"]]
        collection = mydb["kaggle_datasets"]

        dataset_to_be_downloaded = collection.find_one({"downloaded": False})

        if not dataset_to_be_downloaded:
            return "finalize_dag"

        kwargs["ti"].xcom_push(
            key="dataset",
            value={
                "dataset_name": dataset_to_be_downloaded["dataset_name"],
                "dataset_owner": dataset_to_be_downloaded["dataset_owner"],
            },
        )
        logging.info(f"Get the dataset: {dataset_to_be_downloaded['dataset_name']}")
        return "getting_dataset"
    except Exception as e:
        logging.error(f"An exception: {e}")
