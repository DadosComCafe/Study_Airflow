import csv
import logging
from typing import Union


def find_record_with_no_null_value(csv_data) -> dict:
    """Find a record with no null value from the given CSV data. Iterate over the data and try to find a record with a
    valid value over every keys.

    Args:
        csv_data (iterable): Iterable containing the CSV data.

    Returns:
        dict: The first dictionary which corresponds to the record.
    """
    csv_reader = csv.DictReader(csv_data)

    for record in csv_reader:
        no_null_fields = True
        for value in record.values():
            if value is None or value == "":
                no_null_fields = False
                break

        if no_null_fields:
            return record


def return_record_without_null_value(path_of_csv: str) -> dict:
    with open(path_of_csv, mode="r") as file:
        csv_content = file.readlines()

    record_without_null = find_record_with_no_null_value(csv_content)
    return record_without_null


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
    desired_obj = return_record_without_null_value(path_of_csv)
    list_obj_field = [key for key in desired_obj.keys()]
    field_type = {}

    for i in list_obj_field:
        field_type[i] = str(verify_type(desired_obj[i]))

    return field_type


if __name__ == "__main__":
    path_file = "teste_csv.csv"
    print(return_record_without_null_value(path_of_csv=path_file))
