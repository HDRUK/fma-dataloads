import sys
import requests

from requests import RequestException


def get_datasets(url="", access_token="", api_key=""):
    """
    GET: extract the list of datasets from the target server.
    """
    try:
        if access_token:
            headers = {"Authorization": access_token}
        elif api_key:
            headers = {"Authorization": "Basic " + api_key}
        else:
            headers = {}

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            return data["items"]
        else:
            raise RequestException(f"A status code of {response.status_code} was received")

    except Exception as e:
        print("Error retrieving list of datasets: ", e)
        raise


def get_dataset(url="", access_token="", dataset_id=""):
    """
    GET: extract a single dataset from the target server.
    """
    try:
        response = requests.get(url + "/" + str(dataset_id), headers={"Authorization": access_token})

        if response.status_code == 200:
            data = response.json()

            return data
        else:
            raise RequestException(f"A status code of {response.status_code} was received")

    except Exception as e:
        print("Error retrieving single dataset: ", e)
        raise
