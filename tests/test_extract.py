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
    except Exception as error:
        assert (
            str(error)
            == f"Authorisation error: unauthorised 401 error was received from {datasets_url}"
        )


@responses.activate
def test_get_datasets__403():
    """
    Function should raise Exception if status code != 200.
    """
    datasets_url = "http://custodian/datasets"
    auth_token = "invalidAuthJWT"

    responses.add(
        responses.GET,
        datasets_url,
        status=403,
    )

    try:
        get_datasets(datasets_url, auth_token)
    except Exception as error:
        assert (
            str(error)
            == f"Authorisation error: unauthorised 403 error was received from {datasets_url}"
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
    except Exception as error:
        assert (
            str(error)
            == f"Authorisation error: unauthorised 401 error was received from {dataset_url}"
        )


@responses.activate
def test_get_dataset__403():
    """
    Function should raise Exception if status code != 200.
    """
    dataset_url = "http://custodian/datasets"
    auth_token = "testAuthJWT"
    dataset_id = "abc"

    responses.add(
        responses.GET,
        dataset_url + "/" + dataset_id,
        status=403,
    )

    try:
        get_dataset(dataset_url, auth_token, dataset_id)
    except Exception as error:
        assert (
            str(error)
            == f"Authorisation error: unauthorised 403 error was received from {dataset_url}"
        )
