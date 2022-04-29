import responses

from responses import matchers

from functions.auth import *


@responses.activate
def test_get_access_token__200():
    """
    Function should return access token if status code == 200.
    """
    client_id = "validClientId"
    client_secret = "validClientSecret"
    token_url = "http://auth.com/token"

    responses.add(
        responses.POST,
        token_url,
        json={"access_token": "hereIsTheAccessToken"},
        status=200,
        match=[
            matchers.urlencoded_params_matcher(
                {
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                }
            )
        ],
    )

    access_token = get_access_token(token_url, client_id, client_secret)

    assert access_token == "hereIsTheAccessToken"


@responses.activate
def test_get_access_token__401():
    """
    Function should raise Exception if status code != 200.
    """
    client_id = "unauthorisedClientId"
    client_secret = "unauthorisedClientSecret"
    token_url = "http://auth.com/token"

    responses.add(
        responses.POST,
        token_url,
        status=401,
    )

    try:
        get_access_token(token_url, client_id, client_secret)
    except Exception as error:
        assert (
            str(error)
            == f"Authorisation error: 401 error was received from {token_url}"
        )


@responses.activate
def test_get_access_token__403():
    """
    Function should raise Exception if status code != 200.
    """
    client_id = "unauthorisedClientId"
    client_secret = "unauthorisedClientSecret"
    token_url = "http://auth.com/token"

    responses.add(
        responses.POST,
        token_url,
        status=403,
    )

    try:
        get_access_token(token_url, client_id, client_secret)
    except Exception as error:
        assert (
            str(error)
            == f"Authorisation error: 403 error was received from {token_url}"
        )
