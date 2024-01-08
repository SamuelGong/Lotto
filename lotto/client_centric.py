import copy
import numpy as np
import logging
from lotto import base
from lotto.utils.share_memory_handler \
    import META
from infra.utils.share_memory_handler import SAMPLED_CLIENTS
from lotto.primitives.signature \
    import registry as signature_registry
from lotto.primitives.verifiable_random_function \
    import registry as verifiable_random_function_registry

randomness_range = 2 ** (64 * 8)  # 32 bytes


class CliSltSecServer(base.CliSltSecServer):
    def __init__(self, client_id, total_clients,
                 target_num_sampled_clients, mock_sampling, args):
        super(CliSltSecServer, self).__init__(client_id, total_clients,
                                              target_num_sampled_clients,
                                              mock_sampling, args)

    def initialize(self):
        meta_data = self.get_a_shared_value(key=[META])
        meta_data.update({
            "for_verification": {},
            "vrf": {}
        })
        for client_id in range(1, self.total_clients + 1):
            meta_data["vrf"][client_id] = {}
        meta_data["for_ver"] = {}
        self.set_a_shared_value(
            key=[META],
            value=meta_data
        )

    def random_sampling(self, self_sampling_results, round_idx,
                        log_prefix_str, save_result=True):
        logging.info(f"{log_prefix_str} [Lotto] Client-centric sampling started.")

        # logging.info(f"{log_prefix_str} [Lotto] [Debug] {self_sampling_results}")
        primarily_sampled = {}
        for client_id, self_sampling_result in self_sampling_results.items():
            status = self_sampling_result["status"]
            if status:
                randomness_bytes = self_sampling_result["randomness"]
                primarily_sampled[client_id] = int.from_bytes(randomness_bytes, 'big')
        sorted_primarily_sampled \
            = {k: v for k, v in sorted(primarily_sampled.items(),
                                       key=lambda item: item[1])}
        # logging.info(f"{log_prefix_str} [Lotto] [Debug] "
        #              f"Primarily sampled {len(sorted_primarily_sampled)} "
        #              f"clients for Round {round_idx}: {sorted_primarily_sampled}")

        finally_sampled \
            = list(sorted_primarily_sampled.keys())[:self.target_num_sampled_clients]
        logging.info(f"{log_prefix_str} [Lotto] "
                     f"Finally sampled {len(finally_sampled)} "
                     f"clients for Round {round_idx}: {finally_sampled}")

        meta_data = self.get_a_shared_value(key=[META])
        temp = {}
        for client_id in finally_sampled:
            temp[client_id] = {
                'randomness': self_sampling_results[client_id]["randomness"],
                'proof': self_sampling_results[client_id]["proof"]
            }
        meta_data["for_ver"][round_idx] = temp
        self.set_a_shared_value(
            key=[META],
            value=meta_data
        )

        logging.info(f"{log_prefix_str} [Lotto] Client-centric sampling ended.")
        if save_result:
            self.set_a_shared_value(
                key=[SAMPLED_CLIENTS, round_idx],
                value=finally_sampled
            )
        else:
            return finally_sampled

    def get_details_for_client_verification(self, round_idx):
        meta_data = self.get_a_shared_value(key=[META])
        return meta_data["for_ver"][round_idx]


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

        vrf_handler = verifiable_random_function_registry.get(self.args.verifiable_random_function)
        vrf_pri_bytes, vrf_pub_bytes = vrf_handler.generate_keys()
        meta_data["key"][self.client_id]['pub_bytes']["vrf"] = vrf_pub_bytes
        meta_data["key"][self.client_id]['pri_bytes']["vrf"] = vrf_pri_bytes
        self.set_a_shared_value(
            key=[META],
            value=meta_data
        )

    def self_sampling(self, claimed_population, round_idx, log_prefix_str):
        logging.info(f"{log_prefix_str} [Lotto] Client-centric self-sampling started.")
        assert claimed_population >= self.args.min_accepted_population

        vrf_handler = verifiable_random_function_registry.get(self.args.verifiable_random_function)
        meta_data = self.get_a_shared_value(key=[META])
        vrf_pri_bytes = meta_data["key"][self.client_id]['pri_bytes']["vrf"]
        randomness_bytes, proof = vrf_handler.generate_randomness(
            secret_key=vrf_pri_bytes,
            input_bytes=round_idx.to_bytes(32, 'big')
        )
        randomness = int.from_bytes(randomness_bytes, 'big')

        over_selection_factor = self.args.over_selection_factor
        threshold = int(np.floor(over_selection_factor * self.target_num_sampled_clients
                                 * randomness_range / claimed_population))
        logging.info(f"{log_prefix_str} [Lotto] "
                     f"claimed_population: {claimed_population}, "
                     f"normalized threshold: {threshold / randomness_range}.")

        if randomness <= threshold:
            result = {
                'status': True,
                'randomness': randomness_bytes,
                'proof': proof
            }
            logging.info(f"{log_prefix_str} [Lotto] Decided to participate in "
                         f"Round {round_idx} with randomness being {randomness}.")
        else:
            result = {'status': False}
            logging.info(f"{log_prefix_str} [Lotto] Decided not to participate "
                         f"in Round {round_idx}.")
        logging.info(f"{log_prefix_str} [Lotto] Client-centric self-sampling ended.")
        return result

    def verify_sampling_execution(self, actual_sampled_clients, claimed_population,
                                  provided_details, round_idx, log_prefix_str):
        logging.info(f"{log_prefix_str} [Lotto] Client-centric outcome verification started.")

        logging.info(f"[Debug] Actual sampled clients: {actual_sampled_clients}.")
        if not self.mock_sampling:
            assert self.client_id in actual_sampled_clients
        assert len(actual_sampled_clients) == self.target_num_sampled_clients

        pub_bytes_dict = {}
        for client_id in actual_sampled_clients:
            pub_bytes_dict[client_id] = self.get_public_bytes(client_id)["vrf"]

        over_selection_factor = self.args.over_selection_factor
        threshold = int(np.floor(over_selection_factor * self.target_num_sampled_clients
                                 * randomness_range / claimed_population))
        logging.info(f"{log_prefix_str} [Lotto] "
                     f"normalized threshold: {threshold / randomness_range}.")

        vrf_handler = verifiable_random_function_registry.get(self.args.verifiable_random_function)
        for client_id, pub_bytes in pub_bytes_dict.items():
            randomness_bytes = provided_details[client_id]["randomness"]
            vrf_handler.verify_randomness(
                input_bytes=round_idx.to_bytes(32, 'big'),
                public_key=pub_bytes,
                randomness=randomness_bytes,
                proof=provided_details[client_id]["proof"],
            )
            randomness = int.from_bytes(randomness_bytes, 'big')
            assert randomness <= threshold

        signed_outcome = self.sign_outcome(round_idx, actual_sampled_clients, log_prefix_str)
        logging.info(f"{log_prefix_str} [Lotto] Client-centric outcome verification ended.")
        return signed_outcome
