import os
from lotto.primitives.randomness_generator import base


class Handler(base.Handler):
    def __init__(self):
        super().__init__()

    def generate_bytes(self, size=32):
        return os.urandom(size)  # for ease of debugging
