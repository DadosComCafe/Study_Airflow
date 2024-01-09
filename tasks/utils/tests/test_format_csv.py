from format_csv import (
    find_record_with_no_null_value,
    read_csv_data,
    verify_type,
    generate_relation,
)
from unittest import TestCase
from unittest.mock import patch


class TestFindRecordWithNoNullValue(TestCase):
    def setUp(self):
        self.list_of_dictionaries_without_null = [
            {"key1": "value1", "key2": "value2"},
            {"key1": "value3", "key2": "value4"},
        ]
        self.list_of_dictionaries_with_null = [
            {"key1": "value1", "key2": None},
            {"key1": "value3", "key2": "value4"},
        ]
        self.list_of_dictionaries_with_only_null = [
            {"key1": "value1", "key2": None},
            {"key1": None, "key2": "value4"},
        ]

    def test_return_value_without_null(self):
        """Must return the first dictionary from the given list of dictionaries"""
        find_record_obj = find_record_with_no_null_value(
            self.list_of_dictionaries_without_null
        )
        self.assertEqual(find_record_obj, {"key1": "value1", "key2": "value2"})

    def test_return_value_with_null(self):
        """Must return the second dictionary from the given list of dictionaries"""
        find_record_obj = find_record_with_no_null_value(
            self.list_of_dictionaries_with_null
        )
        self.assertEqual(find_record_obj, {"key1": "value3", "key2": "value4"})

    def test_return_value_with_only_null(self):
        """Must return None"""
        find_record_obj = find_record_with_no_null_value(
            self.list_of_dictionaries_with_only_null
        )
        self.assertEqual(find_record_obj, None)


@patch(
    "format_csv.main.csv.DictReader",
    return_value=[
        {"coluna1": "valor1", "coluna2": "valor2"},
        {"coluna1": "valor3", "coluna2": "valor4"},
    ],
)
@patch("builtins.open")
class TestReadCSVData(TestCase):
    def setUp(self):
        self.csv_file = "caminho/do/arquivo.csv"

    def test_return_list_of_dicts(self, mocked_open, mocked_csv):
        """Must return a list of dictionaries"""
        data = read_csv_data(self.csv_file)
        self.assertIsInstance(data, list)
        self.assertIsInstance(data[0], dict)

    def test_return_value(self, mocked_open, mocked_csv):
        """Must return the following list of dictionaries"""
        data = read_csv_data(self.csv_file)
        self.assertEqual(
            data,
            [
                {"coluna1": "valor1", "coluna2": "valor2"},
                {"coluna1": "valor3", "coluna2": "valor4"},
            ],
        )


class TestVerifyType(TestCase):
    def setUp(self):
        self.dict_of_values = {
            "int": "100",
            "float": "100.5",
            "string": "Test",
        }

    def test_return_value(self):
        """Must return the respective type of class outside from the string"""
        self.assertEqual(verify_type(self.dict_of_values["int"]), int)
        self.assertEqual(verify_type(self.dict_of_values["float"]), float)
        self.assertEqual(verify_type(self.dict_of_values["string"]), str)


@patch("format_csv.main.read_csv_data")
@patch(
    "format_csv.main.find_record_with_no_null_value",
    return_value={"key1": "value1", "key2": "value2", "key3": "50"},
)
class TestGenerateRelation(TestCase):
    def setUp(self):
        self.csv_file = "caminho/do/arquivo.csv"

    def test_format_of_return(self, mocked_csv, mocked_not_null):
        """Must return a dictionary instance"""
        generate_relation_obj = generate_relation(self.csv_file)
        self.assertIsInstance(generate_relation_obj, dict)

    def test_content_return(self, mocked_csv, mocked_not_null):
        """Must return a dictionary instance"""
        generate_relation_obj = generate_relation(self.csv_file)
        expected_return = {
            "key1": "<class 'str'>",
            "key2": "<class 'str'>",
            "key3": "<class 'int'>",
        }
        self.assertEqual(generate_relation_obj, expected_return)
