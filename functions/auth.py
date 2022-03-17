import json
import requests

from requests import RequestException
from google.cloud import secretmanager


def get_access_token(token_url="", client_id="", client_secret=""):
    """
    Retrieve the access token from the target server using the supplied client credentials.
    """
    try:
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
        else:
            raise RequestException("An invalid status code was received")

    except Exception as e:
        print("Error retrieving access token: ", e)
        raise


def get_client_secret(secret_name=""):
    """
    Retrieve secret from the Google Secret Manager given a secret name.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()

        response = client.access_secret_version(request={"name": secret_name})

        return json.loads(
            response.payload.data.decode("utf8").replace("'", '"')
        )

    except Exception as e:
        print("Error retrieving secrets from GCP: ", e)
        raise
