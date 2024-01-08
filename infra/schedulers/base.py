import json
import logging
import time
import redis
import pickle
import numpy as np
from functools import reduce
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool as Pool
from infra.utils.share_memory_handler \
    import ShareBase, SCHEDULE, redis_pool, \
    SAMPLED_CLIENTS, BREAK_SCHEDULER_LOOP, ORIGINAL_SAMPLED_CLIENTS
from infra.config import Config

# N_CORES = min(cpu_count(), 16)
N_CORES = cpu_count()
r = redis.Redis(connection_pool=redis_pool)


class PhaseNodePerRound:
    def __init__(self, id, params, num_chunks):
        self.id = id
        self.worker = params["worker"]
        self.predecessor_ids = params["predecessor_ids"]
        self.successor_id = params["successor_id"]
        self.stage = params["stage"]
        self.chunks_completed = [False] * num_chunks

        # chunk_idx (int): phase_idx_list (list of int) (waitee)
        # dependant chunk (int): {
        #     'chunk_idx': chunk_holding_the_resource (int),
        #     'phase_idx_list': phases_holding_the_resource (int)
        # }
        # consider both online and offline mode
        if "resource_dependency" in params:
            self.resource_dependency = params["resource_dependency"]

        # chunk_being_waited (int): {
        #     waiting_chunk (int): list_of_waiting_phases (list)
        # }
        self.waited_by = {}


class Scheduler(ShareBase):
    def __init__(self, infra_instance, log_prefix_str):
        super(Scheduler, self).__init__(client_id=0)
        self.infra_server = infra_instance
        self.log_prefix_str = log_prefix_str
        self.final_online_node_id = None
        self.final_offline_node_id = None

        self.round_dict = {}
        self.offline_round_dict = {}

        self.online_graph_dict = None
        self.offline_graph_dict = None

        self.round = None
        self.num_chunks = None
        self.online_leaf_phases = []
        self.total_clients = Config().clients.total_clients

    def set_num_chunks(self, num_chunks):
        self.num_chunks = num_chunks
        self.find_resource_dependencies()

        # for debug use
        nice_prompt = json.dumps(
            self.online_graph_dict, sort_keys=False, indent=4)
        logging.info(f"graph_dict: {nice_prompt}.")

    def register_graphs(self, graph_dicts):
        """
        sample item in graph_dict (the input):
        {id: {
            worker: [method]
            predecessor_ids: [list of id]
            is_final: [bool, optional]
        }}
        """
        for graph_idx, graph_dict in enumerate(graph_dicts):  # should be a list
            # by default the first one is for online graph
            # the second one, if any, is for offline graph
            if graph_idx == 0:
                mode = "online"
            else:
                mode = "offline"

            for phase_idx, d in graph_dict.items():
                if "is_final" in d and d["is_final"]:
                    if mode == "online":
                        self.final_online_node_id = phase_idx
                    else:  # offline
                        self.final_offline_node_id = phase_idx
                if mode == "online" and not d["predecessor_ids"]:
                    self.online_leaf_phases.append(phase_idx)
                graph_dict[phase_idx]["successor_id"] = None

            # set_successor_id
            for phase_idx, phase_dict in graph_dict.items():
                predecessors = phase_dict['predecessor_ids']
                for p in predecessors:
                    graph_dict[p]["successor_id"] = phase_idx

            if mode == "online":
                self.online_graph_dict = graph_dict
            else:  # offline
                self.offline_graph_dict = graph_dict

    def find_predecessors_with_same_stage(self, phase_idx,
                                          target_stage, diff=False):
        current_phase = self.online_graph_dict[phase_idx]
        if not current_phase["predecessor_ids"]:
            return []

        res = []
        for predecessor_idx in current_phase["predecessor_ids"]:
            predecessor_dict = self.online_graph_dict[predecessor_idx]
            predecessor_stage = predecessor_dict["stage"]

            if predecessor_stage == target_stage:
                if diff:  # need to find same stage that is not adjacent
                    res += [predecessor_idx]
                else:
                    res += self.find_predecessors_with_same_stage(
                        phase_idx=predecessor_idx,
                        target_stage=target_stage
                    )
            else:
                res += self.find_predecessors_with_same_stage(
                    phase_idx=predecessor_idx,
                    target_stage=target_stage,
                    diff=True
                )
        return res

    def find_resource_dependencies(self):
        for phase_idx, d in self.online_graph_dict.items():
            self.online_graph_dict[phase_idx]["resource_dependency"] = {}
            for chunk_idx in range(self.num_chunks):
                self_stage = d["stage"]
                if chunk_idx == 0:
                    predecessors_with_same_stage \
                        = self.find_predecessors_with_same_stage(
                        phase_idx=phase_idx,
                        target_stage=self_stage
                    )
                    self.online_graph_dict[phase_idx]["resource_dependency"]\
                        [chunk_idx] = {
                        'chunk_idx': self.num_chunks - 1,
                        'phase_idx_list': predecessors_with_same_stage
                    }
                else:
                    _phase_idx = phase_idx
                    _phase_dict = d
                    while _phase_dict["successor_id"] is not None:
                        successor_idx = _phase_dict["successor_id"]
                        if successor_idx == self.final_online_node_id:
                            break

                        successor_stage = self.online_graph_dict[
                            successor_idx]["stage"]
                        if not successor_stage == self_stage:
                            break
                        _phase_idx = successor_idx
                        _phase_dict = self.online_graph_dict[_phase_idx]

                    self.online_graph_dict[phase_idx]["resource_dependency"]\
                        [chunk_idx] = {
                        'chunk_idx': chunk_idx - 1,
                        'phase_idx_list': [_phase_idx]
                    }

    def if_resource_available(self, round_idx, chunk_idx,
                              phase_idx, set_waited=False):
        resource_available = True
        phase = self.round_dict[round_idx][phase_idx]

        if phase.resource_dependency \
                and chunk_idx in phase.resource_dependency:
            d = phase.resource_dependency[chunk_idx]
            chunk_idx_to_wait = d["chunk_idx"]
            phase_idx_list_to_wait = d["phase_idx_list"]  # is a list

            for phase_idx_to_wait in phase_idx_list_to_wait:
                phase_to_wait = self.round_dict[round_idx][phase_idx_to_wait]
                if not phase_to_wait.chunks_completed[chunk_idx_to_wait]:
                    resource_available = False
                    if set_waited:
                        if chunk_idx_to_wait not in phase_to_wait.waited_by:
                            phase_to_wait.waited_by[chunk_idx_to_wait] = {}

                        if chunk_idx in phase_to_wait\
                                .waited_by[chunk_idx_to_wait]:
                            self.round_dict[round_idx][phase_idx_to_wait]\
                                .waited_by[chunk_idx_to_wait][chunk_idx]\
                                .append(phase_idx)
                        else:
                            self.round_dict[round_idx][phase_idx_to_wait] \
                                .waited_by[chunk_idx_to_wait][chunk_idx] \
                                = [phase_idx]
                    else:
                        break

        return resource_available

    def if_all_siblings_done(self, round_idx, chunk_idx, phase_idx):
        all_siblings_done = True
        phase = self.round_dict[round_idx][phase_idx]

        for sibling_phase in phase.predecessor_ids:
            sibling = self.round_dict[round_idx][sibling_phase]
            if not sibling.chunks_completed[chunk_idx]:
                all_siblings_done = False
                break

        return all_siblings_done

    def new_an_offline_round(self):
        # We do not have multiple round in an offline process
        # we call it a round for code reusing with online process
        round_idx = -1
        # We do not partition in offline phase
        # so there is conceptually only one chunk
        num_chunks, chunk_idx = 1, 0
        node_dict = {}
        for phase_idx, d in self.offline_graph_dict.items():
            new_node = PhaseNodePerRound(phase_idx, d, num_chunks)
            node_dict[phase_idx] = new_node
        self.offline_round_dict[round_idx] = node_dict

        # currently to accommodate simulation of Lotto
        # here only need to involve all physical clients
        # so below is a walkaround
        from infra.client_samplers.all_inclusive import ClientSampler
        client_sampler = ClientSampler(client_id=0)
        client_sampler.sample(
            candidates=list(range(1, self.infra_server.num_physical_clients + 1)),
            # actual candidates will be determined if client_sampler is trace_driven
            round_idx=round_idx,
            log_prefix_str=self.log_prefix_str
        )

        # the leaf phase is presumably only phase 0
        leaf_phase_id = 0
        self.infra_server.protocol.execute_a_task(
            task_info=(round_idx, chunk_idx, leaf_phase_id),
            type="offline"
        )

    def new_a_round(self, round_idx, num_chunks, presampling=False):
        if presampling:  # sampling clients for the first round
            available_clients = self.infra_server.client_manager.get_available_clients()
            logging.info(f"{self.log_prefix_str} [Round {round_idx}] "
                         f"Available clients: {available_clients}.")

            # Currently do not verify the result for the first round
            # justification: real training can simply start from the second round
            if hasattr(Config().clients.sample, "security"):
                if Config().clients.sample.security.type == "client_centric":
                    # for Lotto: first time's client-centric sampling
                    self_sampling_results \
                        = self.infra_server.protocol.get_record_for_a_phase(
                        round_idx=-1,
                        chunk_idx=0,
                        phase_idx=1
                    )
                    self.infra_server.protocol \
                        .client_sampling_security.random_sampling(
                        self_sampling_results=self_sampling_results,
                        round_idx=round_idx,
                        log_prefix_str=self.log_prefix_str
                    )
                else:  # server_centric
                    self.infra_server.protocol\
                        .client_sampling_security.random_sampling(
                        candidates=available_clients,
                        round_idx=round_idx,
                        log_prefix_str=self.log_prefix_str
                    )
            else:
                self.infra_server.client_sampler.sample(
                    candidates=available_clients,
                    # actual candidates will be determined if client_sampler is trace_driven
                    round_idx=round_idx,
                    log_prefix_str=self.log_prefix_str
                )

            # mock sampling for using consistent clients, thus facilitating fair comparison
            if hasattr(Config().clients.sample, "mock") \
                    and Config().clients.sample.mock:
                logging.info(f"{self.log_prefix_str} Start mocking sampling.")
                original_sampled_clients = self.get_a_shared_value(
                    key=[SAMPLED_CLIENTS, round_idx]
                )  # back up for Lotto's verification
                self.set_a_shared_value(
                    key=[ORIGINAL_SAMPLED_CLIENTS, round_idx],
                    value=original_sampled_clients
                )

                # currently simulate fixed-size sampling
                assert hasattr(Config().clients.sample, "sample_size")
                sample_size = Config().clients.sample.sample_size

                if len(available_clients) >= sample_size \
                        and len(available_clients) > 0:
                    offset = 0  # TODO: avoid hard-coding
                    seed = round_idx + offset
                    logging.info(f"{self.log_prefix_str} Mock sampling using seed {seed}.")
                    rng = np.random.default_rng(seed=seed)
                    sampled_clients = rng.choice(available_clients,
                                                 sample_size,
                                                 replace=False)
                    sampled_clients = sorted([e.item() for e in sampled_clients])
                    self.set_a_shared_value(
                        key=[SAMPLED_CLIENTS, round_idx],
                        value=sampled_clients
                    )
                    logging.info(f"{self.log_prefix_str} End mocking sampling.")
                else:
                    logging.info(f"{self.log_prefix_str} Mock sampling: no clients are sampled "
                                 f"due to insufficient candidates "
                                 f"({len(available_clients)}<{sample_size}).")

        sampled_clients = self.get_a_shared_value(
            key=[SAMPLED_CLIENTS, round_idx]
        )
        if len(sampled_clients) > 0:  # only start if sufficient clients are sampled
            logging.info(f"{self.log_prefix_str} [Round {round_idx}] "
                         f"Sampled clients ({len(sampled_clients)}): {sampled_clients}.")

            # start the application
            node_dict = {}
            for phase_idx, d in self.online_graph_dict.items():
                new_node = PhaseNodePerRound(phase_idx, d, num_chunks)
                node_dict[phase_idx] = new_node
            self.round_dict[round_idx] = node_dict

            for leaf_phase_id in self.online_leaf_phases:
                # leaf_node = self.round_dict[round_idx][leaf_phase_id]
                for chunk_idx in range(self.num_chunks):
                    self.infra_server.protocol.execute_a_task(
                        task_info=(round_idx, chunk_idx, leaf_phase_id),
                        type="online"
                    )
        else:
            assert 0  # should not reach here. If no available clients, should exist earlier

    def schedule_a_process(self, starting_round=None):
        if starting_round is not None:  # online process
            mode = "online"
            logging.info(f"{self.log_prefix_str} Starting round {starting_round}.")
            self.round = starting_round
            self.new_a_round(
                round_idx=self.round,
                num_chunks=self.num_chunks,
                presampling=True
            )
            round_dict = self.round_dict
        else:  # offline process
            mode = "offline"
            logging.info(f"{self.log_prefix_str} Starting offline process.")
            self.new_an_offline_round()
            round_dict = self.offline_round_dict

        sub, ch_dict = self.batch_subscribe_channels(
            d={
                SCHEDULE: False,
                BREAK_SCHEDULER_LOOP: False
            }
        )
        schedule_ch = ch_dict[SCHEDULE]
        break_ch = ch_dict[BREAK_SCHEDULER_LOOP]

        for message in sub.listen():
            raw_data = message['data']
            if not isinstance(raw_data, bytes):
                continue
            channel = message['channel'].decode()
            channel = self.strip_self_channel_prefix(channel)

            if channel == break_ch:  # e.g., break by client sampler due to insufficient available clients
                break
            elif channel == schedule_ch:
                round_idx, chunk_idx, phase_idx = pickle.loads(raw_data)
                # logging.info(f"[Scheduler] {round_idx}/{chunk_idx}/{phase_idx}.")
                current_phase = round_dict[round_idx][phase_idx]
                if current_phase.chunks_completed[chunk_idx]:
                    continue  # redundant signal
                round_dict[round_idx][phase_idx] \
                    .chunks_completed[chunk_idx] = True

                # if there are some phases from other chunks that
                # are waiting for its completion (for resources)
                waited_by = current_phase.waited_by
                if chunk_idx in waited_by:
                    waiter_dict = waited_by[chunk_idx]
                    for _chunk_idx, _phase_idx_list in waiter_dict.items():
                        for _phase_idx in _phase_idx_list:
                            if mode == "online":
                                resource_available = self.if_resource_available(
                                    round_idx=round_idx,
                                    chunk_idx=_chunk_idx,
                                    phase_idx=_phase_idx
                                )
                            else:
                                # offline, where we do not consider
                                # resource usage (and even efficiency)
                                resource_available = True
                            if resource_available:
                                self.infra_server.protocol.execute_a_task(
                                    task_info=(round_idx, _chunk_idx, _phase_idx),
                                    type=mode
                                )

                if mode == "online" and phase_idx == self.final_online_node_id \
                        or mode == "offline" and phase_idx == self.final_offline_node_id:  # if it is the final phase
                    if mode == "online":
                        if not (False in current_phase.chunks_completed) \
                                and round_idx == self.round:
                            logging.info("%s Round %d ended.",
                                         self.log_prefix_str, self.round)
                            if self.round < Config().app.repeat - 1:
                                self.round += 1
                                logging.info("%s Starting round %d.",
                                             self.log_prefix_str, self.round)
                                self.new_a_round(  # other condition can also lead to termination
                                    round_idx=self.round,
                                    num_chunks=self.num_chunks
                                )
                            else:
                                logging.info(f"Reaching the maximum number of rounds.")
                                break
                    else:  # offline
                        if not (False in current_phase.chunks_completed):
                            logging.info("%s Offline process ended.",
                                         self.log_prefix_str)
                            break
                else:
                    # start its successor if possible
                    successor_phase_idx = current_phase.successor_id

                    if mode == "online":
                        # first conditions: the predecessors of the successor
                        # (the so-called siblings) are all done
                        all_siblings_done = self.if_all_siblings_done(
                            round_idx=round_idx,
                            chunk_idx=chunk_idx,
                            phase_idx=successor_phase_idx
                        )
                        if not all_siblings_done:
                            continue

                        if successor_phase_idx == self.final_online_node_id:
                            # second conditions: when it reaches the final chunk
                            if chunk_idx == self.num_chunks - 1:
                                for target_chunk in range(self.num_chunks):
                                    self.infra_server.protocol.execute_a_task(
                                        task_info=(round_idx, target_chunk, successor_phase_idx),
                                        type=mode
                                    )
                        else:
                            resource_available = self.if_resource_available(
                                round_idx=round_idx,
                                chunk_idx=chunk_idx,
                                phase_idx=successor_phase_idx,
                                set_waited=True
                            )
                            if resource_available:
                                self.infra_server.protocol.execute_a_task(
                                    task_info=(round_idx, chunk_idx, successor_phase_idx),
                                    type=mode
                                )
                    else:  # offline
                        self.infra_server.protocol.execute_a_task(
                            task_info=(round_idx, chunk_idx, successor_phase_idx),
                            type=mode
                        )

    def schedule(self, starting_round=0):
        # start offline process for Lotto
        if self.offline_graph_dict is not None:
            self.schedule_a_process()

        # start online process
        self.schedule_a_process(starting_round=starting_round)
