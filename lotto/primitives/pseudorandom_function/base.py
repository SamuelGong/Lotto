from abc import abstractmethod


class Handler:
    def __init__(self):
        pass

    @abstractmethod
    def set_seed(self, seed_bytes):
        """ """

    @abstractmethod
    def generate_randomness(self, input_bytes):
        """ """
