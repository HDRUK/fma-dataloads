import responses

from responses import matchers

from functions.extract import *


@responses.activate
def test_get_datasets__200():
    """
    Function should return list of datasets if status code == 200.
    """
    datasets_url = "http://custodian/datasets"
    auth_token = "testAuthJWT"

    responses.add(
        responses.GET,
        datasets_url,
        json={"items": []},
        status=200,
        match=[
            matchers.header_matcher(
                {
                    "Authorization": auth_token,
                }
            )
        ],
    )

    datasets = get_datasets(datasets_url, auth_token)

    assert datasets == []


@responses.activate
def test_get_datasets__401():
    """
    Function should raise Exception if status code != 200.
    """
    datasets_url = "http://custodian/datasets"
    auth_token = "invalidAuthJWT"

    responses.add(
        responses.GET,
        datasets_url,
        status=401,
    )

    try:
        get_datasets(datasets_url, auth_token)
    except Exception as e:
        assert (
            str(e)
            == "Error extracting list of datasets from http://custodian/datasets: A status code of 401 was received"
        )


@responses.activate
def test_get_dataset__200():
    """
    Function should return single dataset if status code == 200.
    """
    dataset_url = "http://custodian/datasets"
    auth_token = "testAuthJWT"
    dataset_id = "abc"

    responses.add(
        responses.GET,
        dataset_url + "/" + dataset_id,
        json={},
        status=200,
        match=[
            matchers.header_matcher(
                {
                    "Authorization": auth_token,
                }
            ),
        ],
    )

    datasets = get_dataset(dataset_url, auth_token, dataset_id)

    assert datasets == {}


@responses.activate
def test_get_dataset__401():
    """
    Function should raise Exception if status code != 200.
    """
    dataset_url = "http://custodian/datasets"
    auth_token = "testAuthJWT"
    dataset_id = "abc"

    responses.add(
        responses.GET,
        dataset_url + "/" + dataset_id,
        status=401,
    )

    try:
        get_dataset(dataset_url, auth_token, dataset_id)
    except Exception as e:
        assert str(e) == "A status code of 401 was received"
