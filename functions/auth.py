"""
Functions for authorising requests to the server, if required.
"""

import json
import requests

from google.cloud import secretmanager

from .exceptions import *


def get_access_token(
    token_url: str = "", client_id: str = "", client_secret: str = ""
) -> str:
    """
    Retrieve the access token from the target server using the supplied client credentials.
    """

    post = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )

    if post.status_code == 200:
        return post.json()["access_token"]

    if post.status_code in [400, 401, 403]:
        raise AuthError(
            f"Authorisation error: {post.status_code} error was received from {token_url}",
            url=token_url,
        )

    raise RequestError(
        f"An invalid status code was received from {token_url}", url=token_url
    )


def get_client_secret(secret_name: str = "") -> dict:
    """
    Retrieve secret from the Google Secret Manager given a secret name.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()

        response = client.access_secret_version(request={"name": secret_name})

        return json.loads(response.payload.data.decode("utf8").replace("'", '"'))

    except Exception as error:
        raise CriticalError(f"Error retrieving secrets from GCP: {error}") from error
