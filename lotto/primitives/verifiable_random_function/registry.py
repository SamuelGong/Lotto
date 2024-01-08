import logging
from collections import OrderedDict
from lotto.primitives.verifiable_random_function import (
    basic
)

registered_dict = OrderedDict([
    ('basic', basic.Handler)
])


def get(args):
    handler_type = args.type
    if handler_type in registered_dict:
        logging.info("Verifiable random function: %s", handler_type)
        registered_handler = registered_dict[handler_type]()
    else:
        raise ValueError(
            f"No such verifiable random function: {handler_type}"
        )

    return registered_handler
