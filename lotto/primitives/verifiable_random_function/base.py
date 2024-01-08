from abc import abstractmethod


class Handler:
    def __init__(self):
        pass

    @abstractmethod
    def generate_keys(self):
        """ """

    @abstractmethod
    def generate_randomness(self, secret_key, input_bytes):
        """ """

    @abstractmethod
    def verify_randomness(self, input_bytes, public_key, randomness, proof):
        """ """
