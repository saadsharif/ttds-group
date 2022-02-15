

class IndexException(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(message)


class StoreException(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(message)