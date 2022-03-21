class CriticalError(Exception):
    """
    Exception raised when there is a critical script-breaking error.
    """

    def __init__(self, message=""):
        self.message = message
        super().__init__(self, message)

    def __str__(self):
        return self.message
