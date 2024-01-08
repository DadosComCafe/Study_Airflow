import psycopg2
import logging


class HandleSQLDatabase:
    def __init__(self, user, password, host, database=None) -> None:
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.make_connection()

    def make_connection(self):
        self.conn = psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database if self.database else None,
        )
        self.cursor = self.conn.cursor()
        return self.cursor

    def close_connection(self):
        self.cursor.close()
        self.conn.close()

    def check_database_exists(self, database_name: str = "fastfood"):
        sql_query = "SELECT datname FROM pg_database;"

        self.cursor.execute(sql_query)

        databases = self.cursor.fetchall()
        databases = [el[0] for el in databases]

        if database_name in databases:
            return True

        return False

    def create_database(self, database_name: str):
        if not self.check_database_exists(database_name=database_name):
            sql_query = f"CREATE DATABASE {database_name};"
            try:
                self.cursor.execute(sql_query)
                logging.info(f"The database {database_name} has been created!")
            except Exception as e:
                logging.error(f"An error: {e}")

    def create_table(self, tablename: str = "fastfoodnutritionmenu"):
        sql_query = f"""CREATE TABLE IF NOT EXISTS {tablename} (
                id SERIAL Primary Key,
                company text NOT NULL,
                item text NOT NULL,
                calories int,
                caloriesfromfat int,
                totalfat float,
                saturedfat float,
                transfat float,
                cholesterol int,
                sodium int,
                carbs float,
                fiber float,
                sugars float,
                protein float,
                weightwatcherspnts int
        )"""
        try:
            self.cursor.execute(sql_query)
            self.conn.commit()
            logging.info("The table has been created!")
        except Exception as e:
            logging.error(f"An error: {e}")

    def insert_in_table_from_dict(
        self, dict_insert_values: dict, tablename: str = "fastfoodnutritionmenu"
    ):
        try:
            columns = ", ".join(dict_insert_values.keys())
            values = ", ".join(["%s" for _ in range(len(dict_insert_values))])
            sql_query = f"INSERT INTO {tablename} ({columns}) VALUES ({values})"

            self.cursor.execute(sql_query, list(dict_insert_values.values()))
            self.conn.commit()

            logging.info("The data has been inserted into table successfully!")
        except (psycopg2.Error, ValueError) as e:
            logging.error(f"An error: {e}")
            raise


if __name__ == "__main__":
    from decouple import config

    credentials = {
        "user": config("POSTGRESUSER", "postgres"),
        "password": config("POSTGRESPASSWORD", "senha"),
        "host": config("POSTGRESHOST", "localhost"),
        "database": config("POSTGRESDBNAME", "fastfood"),
    }

    postgres_obj = HandleSQLDatabase(**credentials)
    values = {
        "Company": "McDonald’s",
        "Item": "Hamburger",
        "Calories": 250,
        "CaloriesFromFat": 80,
        "TotalFat": 9,
        "SaturatedFat": 3.5,
        "TransFat": 0.5,
        "Cholesterol": 25,
        "Sodium": 520,
        "Carbs": 31,
        "Fiber": 2,
        "Sugars": 6,
        "Protein": 12,
        "WeightWatchersPnts": 247.5,
    }
    try:
        postgres_obj.create_database()
        postgres_obj.insert_in_table_from_dict(dict_insert_values=values)
    except Exception as e:
        logging.error(f"An error: {e}")
