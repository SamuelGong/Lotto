import copy
import pickle
import logging
import numpy as np
from abc import abstractmethod
from lotto.primitives.signature \
    import registry as signature_registry
from lotto.utils.share_memory_handler \
    import ShareBase
from lotto.utils.share_memory_handler \
    import META, EXTERNAL_CLIENT_INFO


# use ShareBase such that Lotto can be used even in multiprocessing scenarios
class CliSltSecBase(ShareBase):
    def __init__(self, client_id, total_clients,
                 target_num_sampled_clients, mock_sampling, args):
        # why we need so many variables in addition to args
        # is because pickle error will appear otherwise
        # due to using args to do conditional branch or anything

        ShareBase.__init__(self, client_id)
        self.client_id = client_id
        self.args = args
        self.total_clients = total_clients
        self.target_num_sampled_clients = target_num_sampled_clients
        self.mock_sampling = mock_sampling
        self.informed = True if hasattr(args, "informed") else False

        meta_data = {"key": {}}
        for client_id in range(1, self.total_clients + 1):
            meta_data["key"][client_id] = {
                "pub_bytes": {},
                "pri_bytes": {}
            }
        if self.informed is True:
            meta_data["internal_client_stats"] = {}

        self.set_a_shared_value(
            key=[META],
            value=meta_data
        )

    def get_public_bytes(self, client_id):
        meta_data = self.get_a_shared_value(key=[META])
        return meta_data["key"][client_id]["pub_bytes"]

    def set_public_bytes(self, pub_bytes_dict):
        meta_data = self.get_a_shared_value(key=[META])
        for client_id, pub_bytes in pub_bytes_dict.items():
            if client_id == self.client_id:
                continue  # no external party can modify my keys
            meta_data["key"][client_id]["pub_bytes"] = pub_bytes
        self.set_a_shared_value(
            key=[META],
            value=meta_data
        )


class CliSltSecServer(CliSltSecBase):
    def __init__(self, client_id, total_clients,
                 target_num_sampled_clients, mock_sampling, args):
        CliSltSecBase.__init__(self, client_id, total_clients,
                               target_num_sampled_clients,
                               mock_sampling, args)
        self.initialize()

    @abstractmethod
    def initialize(self):
        """ """

    def pull_status_quo(self, clients, log_prefix_str):
        registered_clients = []
        updated_clients = []
        meta = self.get_a_shared_value(key=META)
        if "internal_client_stats" in meta:
            internal_client_stats = meta["internal_client_stats"]
        else:
            internal_client_stats = {}

        for client_id in clients:
            external_client_info = self.get_a_shared_value(
                key=[EXTERNAL_CLIENT_INFO, client_id]
            )
            for round_idx, round_dict in external_client_info["round_stats"].items():
                if round_dict["used"] is False:
                    if round_idx == -1:  # for registration
                        if client_id not in internal_client_stats:
                            internal_client_stats[client_id] = {
                                'duration': round_dict["time"],
                                'count': 0,
                                'time_stamp': 0,
                                # bootstrapping
                                'data_quality': external_client_info["training_dataset_size"]
                            }
                        registered_clients.append(client_id)
                    else:  # for update
                        internal_client_stats[client_id].update({
                            'duration': round_dict["time"],
                            'count': internal_client_stats[client_id]['count'] + 1,
                            'time_stamp': round_idx + 1,
                            # tailored for Oort-like sampling
                            'data_quality': external_client_info["training_dataset_size"]
                                            * round_dict["utility"]
                        })
                        updated_clients.append(client_id)

                    round_dict["used"] = True

            # updating the "used" status
            self.set_a_shared_value(
                key=[EXTERNAL_CLIENT_INFO, client_id],
                value=external_client_info
            )
        meta["internal_client_stats"] = internal_client_stats
        self.set_a_shared_value(
            key=META,
            value=meta
        )

        logging.info(f"{log_prefix_str} [Lotto] Status pulled. "
                     f"Clients registered: {registered_clients}, "
                     f"clients updated: {updated_clients}.")

    def refine_population(self, candidates, log_prefix_str):
        updated_clients = []
        meta = self.get_a_shared_value(key=META)
        internal_client_stats = meta["internal_client_stats"]

        # currently only preclude clients whose information is known
        for candidate in candidates:
            time_stamp = internal_client_stats[candidate]["time_stamp"]
            if time_stamp > 0:
                updated_clients.append(candidate)

        clients_to_preclude_list = []
        metric_list = self.args.informed.metric
        for metric in metric_list:
            type = metric.type
            if type == "slowest_speed":
                percentile = metric.percentile
                num_to_preclude = int(np.floor(percentile / 100 * len(updated_clients)))
                if num_to_preclude > 0:
                    updated_client_duration \
                        = [internal_client_stats[e]['duration'] for e in updated_clients]
                    updated_client_duration, updated_clients \
                        = zip(*sorted(zip(updated_client_duration, updated_clients), key=lambda x: x[0]))
                    # NOT [:num_to_preclude]! as we sort them by latency in ascending order
                    clients_to_preclude = updated_clients[-num_to_preclude:]
                    clients_to_preclude_list.append(clients_to_preclude)

                    debug_dict = {
                        k: v for k, v in zip(updated_clients, updated_client_duration)
                    }
            elif type == "poorest_data":
                percentile = metric.percentile
                num_to_preclude = int(np.floor(percentile / 100 * len(updated_clients)))
                if num_to_preclude > 0:
                    updated_client_data_quality \
                        = [internal_client_stats[e]['data_quality'] for e in updated_clients]
                    updated_client_data_quality, updated_clients \
                        = zip(*sorted(zip(updated_client_data_quality, updated_clients), key=lambda x: x[0]))
                    clients_to_preclude = updated_clients[:num_to_preclude]
                    clients_to_preclude_list.append(clients_to_preclude)

                    debug_dict = {
                        k: v for k, v in zip(updated_clients, updated_client_data_quality)
                    }
            elif type == "last_joint":
                percentile = metric.percentile
                num_to_preclude = int(np.floor(percentile / 100 * len(updated_clients)))
                if num_to_preclude > 0:
                    updated_client_duration \
                        = [internal_client_stats[e]['duration'] for e in updated_clients]
                    updated_client_data_quality \
                        = [internal_client_stats[e]['data_quality'] for e in updated_clients]

                    # currently just approximate what Oort basically does
                    # otherwise it will be too complicated
                    # extend to full Oort-like computation only if needed
                    round_penalty = 2

                    updated_client_joint_utility = []
                    min_stat, max_stat = np.min(updated_client_data_quality), np.max(updated_client_data_quality)
                    range_stat = max_stat - min_stat
                    for sys, stat in zip(updated_client_duration, updated_client_data_quality):
                        sc = (stat - min_stat) / range_stat \
                             * (1 / max(1e-4, sys) ** round_penalty)
                        updated_client_joint_utility.append(sc)

                    updated_client_joint_utility, updated_clients \
                        = zip(*sorted(zip(updated_client_joint_utility, updated_clients), key=lambda x: x[0]))
                    clients_to_preclude = updated_clients[:num_to_preclude]
                    clients_to_preclude_list.append(clients_to_preclude)

                    debug_dict = {
                        k: {
                            'sys': v1,
                            'stat': v2,
                            'joint': v3
                        } for k, v1, v2, v3 in zip(
                            updated_clients,
                            updated_client_duration,
                            updated_client_data_quality,
                            updated_client_joint_utility
                        )
                    }
            else:
                raise NotImplementedError

            if num_to_preclude > 0:  # so that debug_dict is defined
                logging.info(f"{log_prefix_str} [Debug] Precluding clients under {type}. "
                             f"debug_dict: {debug_dict}.")

        or_mode = True
        if self.args.informed.type == "and":
            or_mode = False
        clients_to_preclude = set()
        if len(clients_to_preclude_list) > 0:
            clients_to_preclude = set(clients_to_preclude_list[0])
        for e in clients_to_preclude_list[1:]:
            if or_mode:
                clients_to_preclude = clients_to_preclude.union(e)
            else:
                clients_to_preclude = clients_to_preclude.intersection(e)

        clients_to_preclude = sorted(list(clients_to_preclude))
        new_candidates = []
        for candidate in candidates:
            if candidate not in clients_to_preclude:
                new_candidates.append(candidate)
        logging.info(f"{log_prefix_str} [Lotto] Original candidates: {candidates}, "
                     f"{len(clients_to_preclude)} candidates to preclude "
                     f"by {self.args.informed.type} rules with metrics: "
                     f"{[e.type for e in metric_list]}: {clients_to_preclude}.")
        return new_candidates


class CliSltSecClient(CliSltSecBase):
    def __init__(self, client_id, total_clients,
                 target_num_sampled_clients, mock_sampling, args):
        CliSltSecBase.__init__(self, client_id, total_clients,
                               target_num_sampled_clients,
                               mock_sampling, args)
        self.initialize()

    @abstractmethod
    def initialize(self):
        """ """

    def sign_outcome(self, round_idx, actual_sampled_clients, log_prefix_str):
        logging.info(f"{log_prefix_str} [Lotto] Outcome signing started.")

        meta_data = self.get_a_shared_value(key=[META])
        sign_sk_bytes = meta_data["key"][self.client_id]['pri_bytes']["pki"]
        signature_handler = signature_registry.get(self.args.signature)
        sign_sk = signature_handler.bytes_to_private_key(sign_sk_bytes)

        outcome = []
        meta_data = self.get_a_shared_value(key=[META])
        for client_id in actual_sampled_clients:
            pub_bytes = meta_data["key"][client_id]['pub_bytes']["pki"]
            outcome.append(pub_bytes)
        outcome.append(round_idx)
        outcome_bytes = pickle.dumps(outcome)
        signed_outcome = signature_handler.sign(outcome_bytes, sign_sk)

        logging.info(f"{log_prefix_str} [Lotto] Outcome signing ended.")
        return signed_outcome

    def outcome_consistency_check(self, round_idx, actual_sampled_clients,
                                  signed_sampled_clients_dict, log_prefix_str):
        logging.info(f"{log_prefix_str} [Lotto] Consistency check started.")

        signature_clients = list(signed_sampled_clients_dict.keys())
        logging.info(f"[Debug] Signature clients: {signature_clients}.")
        assert len(actual_sampled_clients) == len(signature_clients)
        if not self.mock_sampling:
            for client_id in actual_sampled_clients:
                assert client_id in signature_clients

        meta_data = self.get_a_shared_value(key=[META])
        outcome = []
        for client_id in actual_sampled_clients:
            pub_bytes = meta_data["key"][client_id]['pub_bytes']["pki"]
            outcome.append(pub_bytes)
        outcome.append(round_idx)
        outcome_bytes = pickle.dumps(outcome)

        signature_handler = signature_registry.get(self.args.signature)
        for client_id, signature in signed_sampled_clients_dict.items():
            sign_pk_bytes = meta_data["key"][client_id]['pub_bytes']["pki"]
            sign_pk = signature_handler.bytes_to_public_key(sign_pk_bytes)
            signature_handler.verify(
                signature=signature,
                data=outcome_bytes,
                pk=sign_pk,
            )
        logging.info(f"{log_prefix_str} [Lotto] Consistency check ended.")
