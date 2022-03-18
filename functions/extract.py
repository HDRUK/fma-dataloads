import sys
import requests

from requests import RequestException


def get_datasets(url="", auth_token=""):
    """
    GET: extract the list of datasets from the target server.
    """
    headers = {"Authorization": auth_token}

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            return data["items"]
        else:
            raise RequestException(
                f"A status code of {response.status_code} was received"
            )

    except Exception as e:
        print("Error retrieving list of datasets: ", e)
        raise


def get_dataset(url="", auth_token="", dataset_id=""):
    """
    GET: extract a single dataset from the target server.
    """
    headers = {"Authorization": auth_token}

    response = requests.get(url + "/" + str(dataset_id), headers=headers)

    if response.status_code == 200:
        data = response.json()

        return data
    else:
        raise RequestException(f"A status code of {response.status_code} was received")
