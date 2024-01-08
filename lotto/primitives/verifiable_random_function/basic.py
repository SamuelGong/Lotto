# Reference: https://github.com/nccgroup/draft-irtf-cfrg-vrf-06/blob/master/demo.py

from lotto.primitives.verifiable_random_function import base
from lotto.primitives.verifiable_random_function import ecvrf_edwards25519_sha512_elligator2
import secrets


class Handler(base.Handler):
    def __init__(self):
        super().__init__()

    def generate_keys(self):
        sk = secrets.token_bytes(nbytes=32)
        pk = ecvrf_edwards25519_sha512_elligator2.get_public_key(sk)
        return sk, pk

    def generate_randomness(self, secret_key, input_bytes):
        p_status, proof \
            = ecvrf_edwards25519_sha512_elligator2.ecvrf_prove(secret_key, input_bytes)
        b_status, randomness \
            = ecvrf_edwards25519_sha512_elligator2.ecvrf_proof_to_hash(proof)
        return randomness, proof

    def verify_randomness(self, input_bytes, public_key, randomness, proof):
        result, randomness2 \
            = ecvrf_edwards25519_sha512_elligator2.ecvrf_verify(public_key, proof, input_bytes)
        assert result == "VALID"
        assert randomness == randomness2
