from requests import RequestException


class CriticalError(Exception):
    """
    Exception raised when there is a critical script-breaking error.
    """

    def __init__(self, message: str = ""):
        self.message = message
        super().__init__(self, message)

    def __str__(self):
        return self.message


class AuthError(Exception):
    """
    Exception raised when 401 or 403 is encountered.
    """

    def __init__(self, message: str = "", url: str = ""):
        self.message = message
        self.url = url
        super().__init__(self, message)

    def __str__(self):
        return self.message

    def __url__(self):
        return self.url


class RequestError(RequestException):
    """
    Customised RequestException subclass to record urls.
    """

    def __init__(self, message: str = "", url: str = ""):
        self.message = message
        self.url = url
        super().__init__(self, message)

    def __str__(self):
        return self.message

    def __url__(self):
        return self.url
