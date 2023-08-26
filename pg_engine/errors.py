class DatabaseException(Exception):

    NotConnected = {"message": 'Client is not connected', "code": 100}
    AlreadyConnected = {"message": 'Client is already connected', "code": 101}
    NoValueOperation = {
        "message": 'Operation should have at least one value provided', "code": 400}
    InsertionFailed = {
        "message": "Insertion failed", "code": 400
    }

    def __init__(self, message: str, code: int):
        super().__init__(message=message)
        self.code = code
