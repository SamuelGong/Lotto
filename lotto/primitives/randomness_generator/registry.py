import logging
from collections import OrderedDict
from lotto.primitives.randomness_generator import (
    os_random
)

registered_dict = OrderedDict([
    ('os_random', os_random.Handler)
])


def get(args):
    handler_type = args.type

    if handler_type in registered_dict:
        logging.info("Randomness generator: %s", handler_type)
        registered_handler = registered_dict[handler_type]()
    else:
        raise ValueError(
            f"No such randomness generator: {handler_type}")

    return registered_handler
