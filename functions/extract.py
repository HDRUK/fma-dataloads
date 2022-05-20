"""
Functions for retrieving datasets or a dataset from the target server.
"""

import requests

from .exceptions import *


def get_datasets(url: str = "", headers: dict = None) -> list:
    """
    GET: extract the list of datasets from the target server.
    """
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        return data["items"]

    if response.status_code in [401, 403]:
        raise AuthError(
            f"Authorisation error: unauthorised {response.status_code} error was received from {url}",
            url=url,
        )

    raise RequestError(
        f"Error extracting list of datasets: a status code of {response.status_code} was received from {url}",
        url=url,
    )


def get_dataset(url: str = "", headers: dict = None, dataset_id: str = ""):
    """
    GET: extract a single dataset from the target server.
    """
    response = requests.get(url + "/" + str(dataset_id), headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data

    if response.status_code in [401, 403]:
        raise AuthError(
            f"Authorisation error: unauthorised {response.status_code} error was received from {url}",
            url=url,
        )

    raise RequestError(f"A status code of {response.status_code} was received", url=url)
