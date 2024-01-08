from abc import abstractmethod


class Handler:
    def __init__(self):
        pass

    @abstractmethod
    def generate_bytes(self, size):
        """ """
