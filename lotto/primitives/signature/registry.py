import logging
from collections import OrderedDict
from lotto.primitives.signature import (
    ed25519
)

registered_dict = OrderedDict([
    ('ed25519', ed25519.Handler)
])


def get(args):
    handler_type = args.type

    if handler_type in registered_dict:
        # logging.info("Signature scheme: %s", handler_type)
        registered_handler = registered_dict[handler_type]()
    else:
        raise ValueError(
            f"No such signature scheme: {handler_type}"
        )

    return registered_handler
