from lotto.primitives.pseudorandom_function import base
from cryptography.hazmat.primitives import hashes, hmac


class Handler(base.Handler):
    def __init__(self):
        super().__init__()
        self.h = None

    def set_seed(self, seed_bytes):
        # The seed is recommended to be equal in length to
        # the digest_size of the hash function chosen, which is 32 bytes here
        self.h = hmac.HMAC(seed_bytes, hashes.SHA256())

    def generate_randomness(self, input_bytes):
        self.h.update(input_bytes)
        return self.h.finalize()
