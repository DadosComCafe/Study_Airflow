import os
import logging
from tasks.utils.format_csv.main import generate_relation
from psycopg2 import connect


def translate_python_to_sql(dict_types: dict) -> dict:
    """Translate the python classes into postgres data types.

    Args:
        dict_types (dict): The dictionary of field_name: class_type.
        Ex: {'company': "<class 'str'>", 'calories': "<class 'int'>"}

    Returns:
        dict: The dictionary of field_name: sql_datatype
    """
    new_dict = {}
    for key, value in dict_types.items():
        if value == "<class 'float'>":
            new_dict[key] = "DOUBLE PRECISION"
        if value == "<class 'int'>":
            new_dict[key] = "INTEGER"
        if value == "<class 'str'>":
            new_dict[key] = "VARCHAR"
    return new_dict


def create_table_schema_from_csv(**kwargs):
    """Generates the table schema from the given csv uploaded in the previous task."""
    path_of_csv = kwargs["ti"]
    path_of_csv = path_of_csv.xcom_pull(task_ids="upload_to_gcp", key="csvs_path")
    logging.info(f"Receive the following csvs: {path_of_csv}")
    logging.info(f"Applying generate relation")
    table_schema = [
        generate_relation(path_of_csv[0]),
        path_of_csv[0].replace(".csv", ""),
    ]
    kwargs["ti"].xcom_push(key="table_schema", value=table_schema)


def create_table(table_schema: list[dict, str]) -> str:
    """Create the postgres table from the produced schema in the previous task.

    Args:
        table_schema (list[dict, str]): The produced list in the previous task,
        with a dict returned from `generate_relation()` function and the csv file name.

    Returns:
        str: The query will be used to create the table.
    """
    columns, table_name = table_schema[0], table_schema[1]
    new_columns = {}
    for key, value in columns.items():
        new_columns[
            key.lower()
            .replace("\n", "")
            .replace("(mg)", "")
            .replace("(g)", "")
            .replace(" ", "")
        ] = value

    new_columns = translate_python_to_sql(new_columns)
    columns = ", ".join([f"{column} {new_columns[column]}" for column in new_columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    logging.info(f"The create table query: {create_table_query}")
    return create_table_query


def create_postgres_table_from_schema(postgres_credentials: dict, **kwargs):
    """Receiving the sql to create table from the previous task, this function makes the connection and
    run the generated sql query to create table.

    Args:
        postgres_credentials (dict): The postgres credentials to connect to the database.

    Raises:
        Exception: Raise exception in postgres connection or postgres query execution error.
    """
    table_schema = kwargs["ti"]
    table_schema = table_schema.xcom_pull(
        task_ids="create_table_schema_from_csv", key="table_schema"
    )
    table_schema = create_table(table_schema)
    logging.info(f"Table schema: {table_schema}")
    try:
        conn = connect(
            database=postgres_credentials["dbname"],
            host=postgres_credentials["host"],
            user=postgres_credentials["user"],
            password=postgres_credentials["password"],
            port=postgres_credentials["port"],
        )
        cur = conn.cursor()
        cur.execute(table_schema)
        conn.commit()
        conn.close()
    except Exception as e:
        raise Exception(f"An error: {e}")
