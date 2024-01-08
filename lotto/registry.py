import logging
from collections import OrderedDict


from lotto import (
    server_centric, client_centric
)

registered_servers = OrderedDict([
    ('server_centric', server_centric.CliSltSecServer),
    ('client_centric', client_centric.CliSltSecServer),
])
registered_clients = OrderedDict([
    ('server_centric', server_centric.CliSltSecClient),
    ('client_centric', client_centric.CliSltSecClient),
])


def get(client_id, total_clients, target_num_sampled_clients,
        mock_sampling, args):
    type = args.type

    if client_id == 0:
        if type in registered_servers:
            # logging.info("Client selection security: %s", security_type)
            register_security = registered_servers[type](
                client_id, total_clients, target_num_sampled_clients,
                mock_sampling, args)
        else:
            raise ValueError('No such selection security: {}'.format(type))
    else:
        if type in registered_clients:
            logging.info("Client selection security: %s", type)
            register_security = registered_clients[type](
                client_id, total_clients, target_num_sampled_clients,
                mock_sampling, args)
        else:
            raise ValueError('No such selection security: {}'.format(type))

    return register_security
