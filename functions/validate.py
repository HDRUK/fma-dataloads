import requests

from requests import RequestException
from jsonschema import Draft7Validator


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
                error_details.append(
                    {"error": error.message, "path": list(error.path)}
                )
            return {"dataset": dataset, "errors": error_details}

        return
    except RequestException as e:
        print("Error retrieving datasetv2 validation schema: ", e)
        raise
