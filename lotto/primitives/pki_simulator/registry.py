import logging
from collections import OrderedDict
from lotto.primitives.pki_simulator import (
    elliptic_curve
)

registered_dict = OrderedDict([
    ('elliptic_curve', elliptic_curve.Handler)
])


def get(args):
    handler_type = args.type

    if handler_type in registered_dict:
        logging.info("PKI simulator: %s", handler_type)
        registered_handler = registered_dict[handler_type]()
    else:
        raise ValueError(
            f"No such PKI simulator: {handler_type}")

    return registered_handler
