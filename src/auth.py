import sys
import requests

from requests import RequestException


def get_access_token(token_url="", client_id="", client_secret=""):
    """
    Retrieve the access token from the target server using the supplied client credentials.
    """
    try:
        post = requests.post(
            token_url,
            data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
        )

        if post.status_code == 200:
            return post.json()["access_token"]
        else:
            raise RequestException("An invalid status code was received")

    except Exception as e:
        print("Error retrieving access token: ", e)
        sys.exit(1)
