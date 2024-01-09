import csv
import logging
from typing import Union


def read_csv_data(file_path: str) -> list:
    """A function must be called to handle the csv.DictReader aplication

    Args:
        file_path (str): The file path

    Returns:
        list: A list with the output of DictReader
    """
    with open(file_path, "r") as file:
        return list(csv.DictReader(file))


def find_record_with_no_null_value(csv_data: list) -> dict:
    """Find a record with no null value from the given CSV data. Iterate over the data and try to find a record with a
    valid value over every keys.

    Args:
        csv_data (list): A returned list from read_csv_data() function.

    Returns:
        dict: The first dictionary which corresponds to the record.
    """
    for record in csv_data:
        no_null_fields = all(
            value is not None and value != "" for value in record.values()
        )
        if no_null_fields:
            return record


def verify_type(value: Union[str, float, int]) -> Union[str, float, int]:
    """Verify the type of value and return the corresponding one.

    Args:
        value (Union[str, float, int]): A simple value that desire to verity type.

    Returns:
        Union[str, float, int]: The corresponding type of value.
    """
    if value.isdigit():
        return int
    try:
        float(value)
        return float
    except ValueError:
        return str


def generate_relation(path_of_csv: str) -> dict:
    """Call the functions to get dictionary of field and corresponding type.

    Args:
        path_of_csv (str): The path of csv_file.

    Returns:
        dict: A dictionary of field_name: type.
        Sample: {
            'id': str,
            'age': int,
            'mass': float,
            ...
        }
    """
    logging.info(f"Here {path_of_csv}")
    desired_obj = find_record_with_no_null_value(read_csv_data(path_of_csv))
    list_obj_field = [key for key in desired_obj.keys()]
    field_type = {}

    for i in list_obj_field:
        field_type[i] = str(verify_type(desired_obj[i]))

    return field_type


if __name__ == "__main__":
    path_file = "teste_csv.csv"
    print(find_record_with_no_null_value(read_csv_data(path_file)))
