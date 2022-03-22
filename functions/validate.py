import re
import requests

from requests import RequestException
from jsonschema import Draft7Validator

from functions.exceptions import CriticalError


def validate_json(schema_url, dataset):
    """
    Get the relevant schema and validate a datasetv2 object against the schema.
    """
    try:
        schema = requests.get(schema_url)

        if schema.status_code != 200:
            raise RequestException(
                f"A status code of {schema.status_code} was received"
            )

        validator = Draft7Validator(schema=schema.json())
        errors = list(validator.iter_errors(dataset))

        if len(errors) > 0:
            error_details = []
            for error in errors:
                error_details.append({"error": error.message, "path": list(error.path)})
            dataset["validation_errors"] = error_details
            return dataset

        return
    except RequestException as e:
        raise CriticalError(f"Error retrieving the datasetv2 validation schema: {e}")


def verify_schema_version(schema_url):
    """
    Verify that the supplied schema is either 2.0.0, 2.0.2 or latest.
    """
    allowed_versions = ["2.0.0", "2.0.2", "latest"]
    return bool(re.search("|".join(allowed_versions), schema_url))
