import json
import pytest

from functions.validate import *


@pytest.fixture()
def valid_dataset():
    with open("./tests/mocks/dataset_valid.json") as file:
        return json.load(file)


@pytest.fixture()
def invalid_dataset():
    with open("./tests/mocks/dataset_invalid.json") as file:
        return json.load(file)


@pytest.fixture()
def schema_urls():
    return {
        "2.0.0": "https://raw.githubusercontent.com/HDRUK/schemata/master/schema/dataset/2.0.0/dataset.schema.json",
        "2.0.2": "https://raw.githubusercontent.com/HDRUK/schemata/master/schema/dataset/2.0.2/dataset.schema.json",
        "2.1.0": "https://raw.githubusercontent.com/HDRUK/schemata/master/schema/dataset/latest/dataset.schema.json",
    }


def test_verify_schema_version():
    """
    Function should only return True for schema URL with '2.0.0', '2.0.2' and 'latest'.
    """
    schema_url_correct_1 = "http://abc/latest"
    schema_url_correct_2 = "http://abc/2.0.0"
    schema_url_correct_3 = "http://abc/2.0.2"
    schema_url_bad = "http://abc/not_a_real_schema"

    assert verify_schema_version(schema_url_correct_1)
    assert verify_schema_version(schema_url_correct_2)
    assert verify_schema_version(schema_url_correct_3)
    assert not verify_schema_version(schema_url_bad)


def test_validate_json__valid_dataset(valid_dataset, schema_urls):
    """
    Function should retrieve relevant schema from provided URL and validate JSON successfully.
    Function returns "None" if no errors are encountered.
    """
    is_none = validate_json(schema_urls["2.1.0"], valid_dataset)
    assert is_none is None


def test_validate_json__invalid_dataset(invalid_dataset, schema_urls):
    """
    Function should retrieve relevant schema from provided URL and validate JSON and purposely find errors.
    Function should return the failing datasets with errors appended to the end.
    """
    invalid_dataset = validate_json(schema_urls["2.1.0"], invalid_dataset)
    errors = invalid_dataset["validation_errors"]

    # validation_errors should be an array with 3 found errors
    assert isinstance(errors, list)
    assert len(errors) == 3

    # each error entry in validation_errors should be an object with two keys, path should be a list
    assert isinstance(errors[0], dict)
    assert isinstance(errors[0]["path"], list)
    assert list(errors[0].keys()) == ["error", "path"]

    # errors[0] = revisions should be an array, not an object
    assert errors[0]["error"] == "{} is not of type 'array'"
    assert errors[0]["path"] == ["revisions"]

    # errors[1] = documentation.associatedMedia should be an array of strings, not numbers
    assert errors[1]["error"] == "[0] is not valid under any of the given schemas"
    assert errors[1]["path"] == ["documentation", "associatedMedia"]

    # errors[2] = measuredValue in observations[] should be an integer
    assert (
        errors[2]["error"] == "'THIS SHOULD BE A NUMBER TYPE' is not of type 'integer'"
    )
    assert errors[2]["path"] == ["observations", 0, "measuredValue"]
