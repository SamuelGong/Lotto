import logging
import math

import numpy as np
from infra.config import Config
from infra.client_samplers import base
from infra.utils.share_memory_handler \
    import SAMPLED_CLIENTS, CLIENT_STATS


INTERNAL_CLIENT_STATS = "internal_client_stats"


class ClientSampler(base.ClientSampler):
    def __init__(self, client_id):
        super(ClientSampler, self).__init__(client_id=client_id)
        seed = Config().clients.sample.seed
        np.random.seed(seed)

        self.mode = Config().clients.sample.mode
        self.num_sampled_client_upperbound = None
        self.sampling_rate_upperbound = None
        self.preclude_clients = False
        if hasattr(Config().clients.sample, "preclude_clients"):
            self.preclude_clients = True
            self.set_a_shared_value(
                key=INTERNAL_CLIENT_STATS,
                value={}
            )
            # self.client_stats = {}

        if self.mode == "fixed_sample_size":
            assert hasattr(Config().clients.sample, "sample_size")
            self.sample_size = Config().clients.sample.sample_size
            self.worst_online_frac = Config().clients.worst_online_frac
            self.num_worst_online_clients \
                = int(np.floor(self.total_clients * self.worst_online_frac))

            self.num_sampled_client_upperbound = self.sample_size
            self.sampling_rate_upperbound = self.sample_size \
                                            / self.num_worst_online_clients

            logging.info(f"[Uniform] Uniformly at random sample {self.sample_size} "
                         f"out of all available clients without replacement.")
        elif self.mode == "sampling_rate_upperbounded":
            assert hasattr(Config().clients.sample, "sampling_rate_upperbound")

            self.sampling_rate_upperbound \
                = Config().clients.sample.sampling_rate_upperbound
            self.num_sampled_client_upperbound = int(np.floor(
                self.total_clients * self.sampling_rate_upperbound
            ))

            logging.info(f"[Uniform] Uniformly at random sample a subset of "
                         f"all available clients without replacement, "
                         f"where the sampling rate for each client "
                         f"does not exceed {self.sampling_rate_upperbound}.")

    def get_num_sampled_clients_upperbound(self):
        return self.num_sampled_client_upperbound

    def get_sampling_rate_upperbound(self):
        return self.sampling_rate_upperbound

    def pull_status_quo(self, clients):
        registered_clients = []
        updated_clients = []
        client_stats = self.get_a_shared_value(key=INTERNAL_CLIENT_STATS)

        for client_id in clients:
            client_dict = self.get_a_shared_value(
                key=[CLIENT_STATS, client_id]
            )
            for round_idx, round_dict in client_dict["round_stats"].items():
                if round_dict["used"] is False:
                    if round_idx == -1:  # for registration
                        if client_id not in client_stats:
                            client_stats[client_id] = {
                                'duration': round_dict["time"],
                                'count': 0,
                                'time_stamp': 0,
                                # bootstrapping
                                'data_quality': client_dict["training_dataset_size"]
                            }
                        registered_clients.append(client_id)
                    else:  # for update
                        client_stats[client_id].update({
                            'duration': round_dict["time"],
                            'count': client_stats[client_id]['count'] + 1,
                            'time_stamp': round_idx + 1,
                            # tailored for Oort-like sampling
                            'data_quality': client_dict["training_dataset_size"]
                                            * round_dict["utility"]
                        })
                        updated_clients.append(client_id)

                    round_dict["used"] = True

            # updating the "used" status
            self.set_a_shared_value(
                key=[CLIENT_STATS, client_id],
                value=client_dict
            )
        self.set_a_shared_value(
            key=INTERNAL_CLIENT_STATS,
            value=client_stats
        )

        logging.info(f"[Uniform] Status pulled. Clients registered: {registered_clients}, "
                     f"clients updated: {updated_clients}.")

    def do_preclude_clients(self, candidates, log_prefix_str):
        updated_clients = []
        client_stats = self.get_a_shared_value(key=INTERNAL_CLIENT_STATS)

        # currently only preclude clients whose information is known
        for candidate in candidates:
            time_stamp = client_stats[candidate]["time_stamp"]
            if time_stamp > 0:
                updated_clients.append(candidate)

        clients_to_preclude_list = []
        type_config_list = Config().clients.sample.preclude_clients
        for type_config in type_config_list:
            type = type_config.type
            if type == "slowest_speed":
                percentile = type_config.percentile
                num_to_preclude = int(np.floor(percentile / 100 * len(updated_clients)))
                if num_to_preclude > 0:
                    updated_client_duration \
                        = [client_stats[e]['duration'] for e in updated_clients]
                    updated_client_duration, updated_clients \
                        = zip(*sorted(zip(updated_client_duration, updated_clients), key=lambda x: x[0]))
                    # NOT [:num_to_preclude]! as we sort them by latency in ascending order
                    clients_to_preclude = updated_clients[-num_to_preclude:]
                    clients_to_preclude_list.append(clients_to_preclude)

                    debug_dict = {
                        k: v for k, v in zip(updated_clients, updated_client_duration)
                    }
            elif type == "poorest_data":
                percentile = type_config.percentile
                num_to_preclude = int(np.floor(percentile / 100 * len(updated_clients)))
                if num_to_preclude > 0:
                    updated_client_data_quality \
                        = [client_stats[e]['data_quality'] for e in updated_clients]
                    updated_client_data_quality, updated_clients \
                        = zip(*sorted(zip(updated_client_data_quality, updated_clients), key=lambda x: x[0]))
                    clients_to_preclude = updated_clients[:num_to_preclude]
                    clients_to_preclude_list.append(clients_to_preclude)

                    debug_dict = {
                        k: v for k, v in zip(updated_clients, updated_client_data_quality)
                    }
            elif type == "last_joint":
                percentile = type_config.percentile
                num_to_preclude = int(np.floor(percentile / 100 * len(updated_clients)))
                if num_to_preclude > 0:
                    updated_client_duration \
                        = [client_stats[e]['duration'] for e in updated_clients]
                    updated_client_data_quality \
                        = [client_stats[e]['data_quality'] for e in updated_clients]

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
        if hasattr(Config().clients.sample, "preclude_clients_and") \
                and Config().clients.sample.preclude_clients_and:
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
        logging.info(f"{log_prefix_str} [Uniform] Original candidates: {candidates}, "
                     f"{len(clients_to_preclude)} candidates to preclude "
                     f"by rules: {[e.type for e in type_config_list]}: {clients_to_preclude}.")
        return new_candidates

    def sample(self, candidates, round_idx, log_prefix_str,
               seed=None, save_result=True):
        if self.preclude_clients:
            self.pull_status_quo(candidates)
            candidates = self.do_preclude_clients(candidates, log_prefix_str)

        if self.mode == "fixed_sample_size":
            sample_size = self.sample_size
        else:  # self.mode == "sampling_rate_upperbounded"
            sample_size \
                = int(np.floor(self.sampling_rate_upperbound * len(candidates)))

        if len(candidates) >= sample_size and len(candidates) > 0:
            if seed is None:
                sampled_clients = np.random.choice(candidates,
                                                   sample_size,
                                                   replace=False)
            else:  # originally for Lotto's verifiable sampling; should be outdated
                logging.info(f"{log_prefix_str} Sampling using seed {seed}.")
                rng = np.random.default_rng(seed=seed)
                sampled_clients = rng.choice(candidates,
                                             sample_size,
                                             replace=False)
            # for serialization in multiprocessing
            sampled_clients = sorted([e.item() for e in sampled_clients])
            if save_result:
                self.set_a_shared_value(
                    key=[SAMPLED_CLIENTS, round_idx],
                    value=sampled_clients
                )
            else:
                return sampled_clients
        else:  # has to stop due to privacy and any other practical concerns
            logging.info(f"[Uniform] No clients are sampled "
                         f"due to insufficient candidates "
                         f"({len(candidates)}<{sample_size}).")
