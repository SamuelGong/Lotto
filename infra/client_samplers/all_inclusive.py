import logging
from infra.client_samplers import base
from infra.utils.share_memory_handler import SAMPLED_CLIENTS


class ClientSampler(base.ClientSampler):
    def __init__(self, client_id):
        super(ClientSampler, self).__init__(client_id=client_id)
        logging.info(f"[All Inclusive] Sample all available "
                     f"clients at each round.")

    def sample(self, candidates, round_idx, log_prefix_str, save_result=True):
        if len(candidates) > 0:
            if save_result:
                self.set_a_shared_value(
                    key=[SAMPLED_CLIENTS, round_idx],
                    value=candidates
                )
            else:
                return candidates
        else:
            logging.info(f"{log_prefix_str} No clients are sampled "
                         f"due to no candidates.")

    def get_sampling_rate_upperbound(self):
        return 1.0

    def get_num_sampled_clients_upperbound(self):
        return self.total_clients
