"""
Functions for retrieving datasets or a datasets from the target server.
"""

import requests

from requests import RequestException

from .exceptions import CriticalError


def get_datasets(url: str = "", auth_token: str = "") -> list:
    """
    GET: extract the list of datasets from the target server.
    """
    headers = {"Authorization": auth_token}

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            return data["items"]

        raise RequestException(f"A status code of {response.status_code} was received")

    except Exception as error:
        raise CriticalError(
            f"Error extracting list of datasets from {url}: {error}"
        ) from error


def get_dataset(url: str = "", auth_token: str = "", dataset_id: str = ""):
    """
    GET: extract a single dataset from the target server.
    """
    headers = {"Authorization": auth_token}

    response = requests.get(url + "/" + str(dataset_id), headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data

    if response.status_code == 500:
        raise CriticalError(f"500 error received from {url}")

    raise RequestException(f"A status code of {response.status_code} was received")
