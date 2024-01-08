import numpy as np
import logging
from lotto import base
from lotto.utils.share_memory_handler \
    import META
from infra.utils.share_memory_handler import SAMPLED_CLIENTS
from lotto.primitives.signature \
    import registry as signature_registry
from lotto.primitives.pseudorandom_function \
    import registry as pseudorandom_function_registry
from lotto.primitives.randomness_generator \
    import registry as randomness_generator_registry

randomness_range = 2 ** (32 * 8)  # 32 bytes


class CliSltSecServer(base.CliSltSecServer):
    def __init__(self, client_id, total_clients,
                 target_num_sampled_clients, mock_sampling, args):
        super(CliSltSecServer, self).__init__(client_id, total_clients,
                                              target_num_sampled_clients,
                                              mock_sampling, args)

    def initialize(self):
        pass

    def random_sampling(self, candidates, round_idx,
                        log_prefix_str, save_result=True):
        logging.info(f"{log_prefix_str} [Lotto] Server-centric sampling started.")

        if self.informed:
            self.pull_status_quo(candidates, log_prefix_str)
            candidates = self.refine_population(candidates, log_prefix_str)
            logging.info(f"{log_prefix_str} [Lotto] Population refined.")

        prf_key_bytes_dict = {
            client_id: self.get_public_bytes(client_id)["prf"] for client_id in candidates
        }
        prf_handler = pseudorandom_function_registry.get(self.args.pseudorandom_function)

        randomness_dict = {}
        for client_id, prf_key_bytes in prf_key_bytes_dict.items():
            prf_handler.set_seed(prf_key_bytes)
            randomness = prf_handler.generate_randomness(round_idx.to_bytes(32, 'big'))
            randomness = int.from_bytes(randomness, 'big')
            randomness_dict[client_id] = randomness
        # logging.info(f"{log_prefix_str} [Lotto] [Debug] "
        #              f"Generated randomness for sampling: {randomness_dict}")

        over_selection_factor = self.args.over_selection_factor
        num_available_clients = len(candidates)
        threshold = int(np.floor(over_selection_factor * self.target_num_sampled_clients
                                 *  randomness_range / num_available_clients))
        logging.info(f"{log_prefix_str} [Lotto] "
                     f"num_available_clients: {num_available_clients}, "
                     f"normalized threshold: {threshold / randomness_range}.")

        primarily_sampled = {}
        for client_id, randomness in randomness_dict.items():
            if randomness <= threshold:
                primarily_sampled.update({client_id: randomness})
        sorted_primarily_sampled \
            = {k: v for k, v in sorted(primarily_sampled.items(),
                                       key=lambda item: item[1])}
        # logging.info(f"{log_prefix_str} [Lotto] [Debug] "
        #              f"Primarily sampled {len(sorted_primarily_sampled)} "
        #              f"clients for Round {round_idx}: {sorted_primarily_sampled}")

        finally_sampled = list(sorted_primarily_sampled.keys())[:self.target_num_sampled_clients]
        logging.info(f"{log_prefix_str} [Lotto] "
                     f"Finally sampled {len(finally_sampled)} "
                     f"clients for Round {round_idx}: {finally_sampled}")

        logging.info(f"{log_prefix_str} [Lotto] Server-centric sampling ended.")
        if save_result:
            self.set_a_shared_value(
                key=[SAMPLED_CLIENTS, round_idx],
                value=finally_sampled
            )
        else:
            return finally_sampled


class CliSltSecClient(base.CliSltSecClient):
    def __init__(self, client_id, total_clients,
                 target_num_sampled_clients, mock_sampling, args):
        super(CliSltSecClient, self).__init__(client_id, total_clients,
                                              target_num_sampled_clients,
                                              mock_sampling, args)

    def initialize(self):
        meta_data = self.get_a_shared_value(key=[META])

        # for simulating a pki
        # the registration key should also be reused for signature
        signature_handler = signature_registry.get(self.args.signature)
        sk, pk = signature_handler.generate_key_pairs()
        pub_bytes = signature_handler.public_key_to_bytes(pk)
        pri_bytes = signature_handler.secret_key_to_bytes(sk)
        meta_data["key"][self.client_id]['pub_bytes']["pki"] = pub_bytes
        meta_data["key"][self.client_id]['pri_bytes']["pki"] = pri_bytes

        randomness_simulator = randomness_generator_registry.get(
            self.args.randomness_simulator)
        random_bytes = randomness_simulator.generate_bytes()
        meta_data["key"][self.client_id]['pub_bytes']["prf"] = random_bytes

        self.set_a_shared_value(
            key=[META],
            value=meta_data
        )

    def verify_sampling_execution(self, actual_sampled_clients,
                                  claimed_population, round_idx, log_prefix_str):
        logging.info(f"{log_prefix_str} [Lotto] Server-centric outcome verification started.")
        if not self.mock_sampling:
            assert self.client_id in actual_sampled_clients
        assert len(actual_sampled_clients) == self.target_num_sampled_clients
        assert claimed_population >= self.args.min_accepted_population

        prf_key_bytes_dict = {
            client_id: self.get_public_bytes(client_id)["prf"] for client_id in actual_sampled_clients
        }

        over_selection_factor = self.args.over_selection_factor
        threshold = int(np.floor(over_selection_factor * self.target_num_sampled_clients
                                 * randomness_range / claimed_population))
        logging.info(f"{log_prefix_str} [Lotto] "
                     f"normalized threshold: {threshold / randomness_range}.")

        prf_handler = pseudorandom_function_registry.get(self.args.pseudorandom_function)
        for client_id, prf_key_bytes in prf_key_bytes_dict.items():
            prf_handler.set_seed(prf_key_bytes)
            randomness = prf_handler.generate_randomness(round_idx.to_bytes(32, 'big'))
            randomness = int.from_bytes(randomness, 'big')
            assert randomness <= threshold

        signed_outcome = self.sign_outcome(round_idx, actual_sampled_clients, log_prefix_str)
        logging.info(f"{log_prefix_str} [Lotto] Server-centric outcome verification ended.")
        return signed_outcome
