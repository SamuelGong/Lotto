from abc import abstractmethod


class Handler:
    def __init__(self):
        pass

    @staticmethod
    def generate_key_pairs():
        """ """

    @staticmethod
    def public_key_to_bytes(pk):
        """ """

    @staticmethod
    def bytes_to_public_key(pb):
        """ """

    @staticmethod
    def secret_key_to_bytes(sk):
        """ """

    @staticmethod
    def bytes_to_private_key(sb):
        """ """

    @staticmethod
    def sign(data, sk):
        """ """

    @staticmethod
    def verify(signature, data, pk):
        """ """
