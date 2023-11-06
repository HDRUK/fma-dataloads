"""
Functions for retrieving datasets or a dataset from the target server.
"""
import logging
import requests
import json

from .exceptions import *


def get_datasets(url: str = "", headers: dict = None) -> list:
    """
    GET: extract the list of datasets from the target server.
    """

    response = requests.get(url, headers=headers)
    response.encoding = 'utf-8-sig'

    if response.status_code == 200:
        data = response.json()

        # return data["items"]
        logging.info(json.dumps(data, ensure_ascii=True))
        json_data = json.loads(json.dumps(data, ensure_ascii=True))
        return json_data["items"]

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

    updated_url = ''

    #  optional
    # if url.find("?") != -1:
    #     updated_url = url[:url.find("?")] + "/" + str(dataset_id) + url[url.find("?"):]
    # else:
    #     updated_url = url + "/" + str(dataset_id)

    updated_url = url.replace("{id}", str(dataset_id))
    
    print("get dataset url", updated_url)

    response = requests.get(updated_url, headers=headers)
    response.encoding = 'utf-8'

    if response.status_code == 200:
        data = response.json()

        # return data
        logging.info(json.dumps(data, ensure_ascii=True))
        json_data = json.loads(json.dumps(data, ensure_ascii=True))
        return json_data

    if response.status_code in [401, 403]:
        raise AuthError(
            f"Authorisation error: unauthorised {response.status_code} error was received from {url}",
            url=url,
        )

    raise RequestError(f"A status code of {response.status_code} was received", url=url)
