import pickle
import logging
import os
import time
import torch
import numpy as np
from PIL import Image
from torchvision import transforms
from infra.config import Config
from infra.apps.federated_learning.datasources import base
from infra.apps.federated_learning.datasources.common import CustomDictDataset


class DataSource(base.DataSource):
    def __init__(self, client_id, quiet=False):
        super().__init__(client_id, quiet)

        _path = Config().app.data.data_path \
            if hasattr(Config().app.data, "data_path") \
            else "./data"
        root_path = os.path.join(_path, 'openImage', 'packaged_data')

        data_urls = {}
        data_dirs = {}
        if self.client_id == 0:
            # If we are on the federated learning server
            data_dirs[self.client_id] = os.path.join(root_path, 'test')
            data_urls[self.client_id] = "https://jiangzhifeng.s3.us-east-2.amazonaws.com/openImage/test/" \
                                        + str(self.client_id) + ".tar"
        else:
            data_dirs[client_id] = os.path.join(root_path, 'train')
            data_urls[self.client_id] = "https://jiangzhifeng.s3.us-east-2.amazonaws.com/openImage/train/" \
                                        + str(self.client_id) + ".tar"

            if hasattr(Config().app.data, "augment") \
                    and Config().app.data.augment.type == "simple":
                repeat = Config().app.data.augment.repeat
                total_clients = Config().clients.total_clients
                for i in range(0, repeat):
                    client_id = (i + 1) * total_clients + self.client_id
                    data_urls[client_id] = "https://jiangzhifeng.s3.us-east-2.amazonaws.com/openImage/train/" \
                                           + str(client_id) + ".tar"

        if hasattr(Config(), "simulation") \
                and Config().simulation.type == "simple":
            to_notify_clients = False
            signal_file_path = os.path.join(_path, "openimage-signal.txt")
            if not os.path.isfile(signal_file_path):
                if self.client_id == 0:
                    to_notify_clients = True

                    total_clients = Config().clients.total_clients
                    for cid in range(1, total_clients + 1):
                        data_dirs[cid] = os.path.join(root_path, 'train')
                        data_urls[cid] = "https://jiangzhifeng.s3.us-east-2.amazonaws.com/openImage/train/" \
                                         + str(cid) + ".tar"
                    logging.info(f"[Simulation] Fetching the data...")
                else:
                    # wait until the server end downloads all the folders
                    waited_sec = 0
                    while not os.path.isfile(signal_file_path):
                        logging.info(f"[Simulation] [{waited_sec}s] Client #{self.client_id} "
                                     f"waiting for the server to complete the fetching.")
                        time.sleep(60)
                        waited_sec += 60

        for client_id, data_url in data_urls.items():
            data_dir = data_dirs[client_id]
            if not os.path.exists(os.path.join(data_dir, str(client_id))):
                logging.info(
                    f"Downloading the openImage dataset for client {client_id}. "
                )
                self.download(url=data_url, data_path=data_dir, quiet=self.quiet)

        if hasattr(Config(), "simulation") \
                and Config().simulation.type == "simple":
            if self.client_id == 0 and to_notify_clients:
                logging.info(f"[Simulation] Fetched successfully. "
                             f"Notifying the clients to proceed...")
                with open(signal_file_path, "w") as fout:
                    fout.writelines(["Downloaded."])

        loaded_data = {"x": [], "y": []}
        for client_id in data_urls.keys():
            data_dir = data_dirs[client_id]
            _data = DataSource.read_data(
                file_path=os.path.join(data_dir, str(client_id)))
            loaded_data["x"] += _data["x"]
            loaded_data["y"] += _data["y"]

        logging.info(f"Physical client(s)' data loaded: {list(data_urls.keys())}.")

        _transform = transforms.Compose([
            transforms.ToPILImage(),
            # transforms.RandomResizedCrop(224),
            transforms.Resize((256, 256)),
            # transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            # transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
            transforms.Normalize((0.4914, 0.4822, 0.4465),
                                 (0.2023, 0.1994, 0.2010)),
        ])

        # Currently we are using c5.4xlarge as the server in our used cluster
        # because it has no GPUs, evaluation of aggregated models can take
        # a long time period if the size of testing dataset is too large.
        # However, in real environment, it is not uncommon for the server to have GPU.
        # To recover a reasonable server runtime overhead in a used cluster, we thus do a down-sampling.
        # Note that server's testing datasets are IID and we are thus safe to do that.
        # if self.client_id == 0 and not torch.cuda.is_available():
        if self.client_id == 0:  # TODO: avoid hard-coding
            dataset_len = len(loaded_data["y"])
            maximum_reserved_samples = 1113  # random sampling
            if dataset_len > maximum_reserved_samples:
                logging.info(f"Original size of data partition: {dataset_len}.")
                new_x = []
                new_y = []
                rng = np.random.default_rng(seed=1)  # TODO: avoid hard-coding
                selected_indices = rng.choice(
                    dataset_len, maximum_reserved_samples, replace=False
                )
                for idx in selected_indices:
                    new_x.append(loaded_data["x"][idx])
                    new_y.append(loaded_data["y"][idx])
                loaded_data["x"] = new_x
                loaded_data["y"] = new_y
        # Currently we are using c5.xlarge as the client in our used cluster
        # because it has limited memory (8GB memory)
        # loading too large dataset can cause OOM error that kills the training process
        else: # TODO: avoid hard-coding
            dataset_len = len(loaded_data["y"])
            maximum_reserved_samples = 1200  # random sampling
            if dataset_len > maximum_reserved_samples:
                logging.info(f"Original size of data partition: {dataset_len}.")
                new_x = []
                new_y = []
                rng = np.random.default_rng(seed=1)  # TODO: avoid hard-coding
                selected_indices = rng.choice(
                    dataset_len, maximum_reserved_samples, replace=False
                )
                for idx in selected_indices:
                    new_x.append(loaded_data["x"][idx])
                    new_y.append(loaded_data["y"][idx])
                loaded_data["x"] = new_x
                loaded_data["y"] = new_y

        dataset = CustomDictDataset(loaded_data=loaded_data,
                                    transform=_transform)

        if self.client_id == 0:
            self.testset = dataset
        else:
            self.trainset = dataset

        self.confirm_and_print_partition_size()

    @staticmethod
    def read_data(file_path):
        """ Reading the dataset specific to a client_id. """
        # file_path is actually a directory
        sample_list = []
        sample_path_list = []
        for f in os.listdir(file_path):
            if f == "sample_label_map":
                sample_label_map_file = os.path.join(file_path, f)
                with open(sample_label_map_file, 'rb') as fin:
                    sample_label_map = pickle.load(fin)
            elif "jpg":
                sample_path_list.append(f)
                image_file = os.path.join(file_path, f)

                image = Image.open(image_file)
                if image.mode != 'RGB':  # to avoid shape error
                    image = image.convert('RGB')
                sample = np.asarray(image)
                sample_list.append(sample)

        label_list = []
        for sample_path in sample_path_list:
            label_list.append(sample_label_map[sample_path])
        loaded_data = {'x': sample_list, 'y': label_list}
        return loaded_data
