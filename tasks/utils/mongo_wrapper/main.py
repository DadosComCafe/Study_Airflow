from pymongo import MongoClient, database, mongo_client


class Mongo:
    def __init__(self, credentials: dict) -> None:
        """Class to handle mongodb connection object

        Args:
            credentials (dict): The database credentials
        """
        self.credentials = credentials
        self.connection_config = f"mongodb://{self.credentials['user']}:{self.credentials['password']}@{self.credentials['host']}:{self.credentials['port']}/"
        self.client = None
        self.db_connection = None

    def connect_to_mongo(self) -> None:
        """Make the mongodb connection"""
        self.client = MongoClient(self.connection_config)

    def get_connection(self) -> database.Database:
        """Get the mongodb connection object

        Returns:
            database.Database: The client object of the respective db_name
        """
        return self.db_connection

    def get_client(self) -> mongo_client.MongoClient:
        """Get the mongodb client object

        Returns:
            mongo_client.MongoClient: The client object
        """
        self.connect_to_mongo()
        return self.client


if __name__ == "__main__":
    from decouple import config

    credentials = {
        "user": config("MONGOUSER"),
        "password": config("MONGOPASSWORD"),
        "host": config("MONGOHOST"),
        "port": config("MONGOPORT"),
        "db_name": config("MONGODBNAME"),
    }
    mongo_obj = Mongo(credentials=credentials)
    mongo_obj.connect_to_mongo()
    connection_obj = mongo_obj.get_connection()
    client_obj = mongo_obj.get_client()
