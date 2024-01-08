from lotto.primitives.signature import base
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519


class Handler(base.Handler):
    def __init__(self):
        super().__init__()

    @staticmethod
    def generate_key_pairs():
        sk = ed25519.Ed25519PrivateKey.generate()
        pk = sk.public_key()
        return sk, pk

    @staticmethod
    def public_key_to_bytes(pk):
        return pk.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )

    @staticmethod
    def bytes_to_public_key(pb):
        return ed25519.Ed25519PublicKey.from_public_bytes(pb)

    @staticmethod
    def secret_key_to_bytes(sk):
        return sk.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption()
        )

    @staticmethod
    def bytes_to_private_key(pb):
        return ed25519.Ed25519PrivateKey.from_private_bytes(pb)

    @staticmethod
    def sign(data, sk):
        # the result is 64 bytes
        return sk.sign(data)

    @staticmethod
    def verify(signature, data, pk):
        pk.verify(signature, data)
