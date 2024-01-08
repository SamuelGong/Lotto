import logging
from lotto.primitives.pki_simulator import base
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization


class Handler(base.Handler):
    def __init__(self):
        super().__init__()
        pass

    @staticmethod
    def generate_key_pairs():
        sk = ec.generate_private_key(
            curve=ec.SECP384R1(),
        )
        pk = sk.public_key()
        return sk, pk

    @staticmethod
    def secret_key_to_bytes(sk):
        return sk.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @staticmethod
    def public_key_to_bytes(pk):
        return pk.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

    @staticmethod
    def bytes_to_secret_key(bytes):
        return serialization.load_pem_private_key(
            data=bytes,
            password=None
        )

    @staticmethod
    def bytes_to_public_key(bytes):
        return serialization.load_pem_public_key(
            data=bytes
        )
