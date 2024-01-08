import pickle
import logging
import os
import torch
from torch.utils.data import Dataset
from infra.config import Config
from infra.apps.federated_learning.datasources import base


class CustomTextDataset(Dataset):
    def __init__(self, loaded_data, transform=None):
        super().__init__()
        self.loaded_data = loaded_data
        self.transform = transform

    def __getitem__(self, index):
        sample = self.loaded_data['x'][index]
        sample = torch.tensor(sample, dtype=torch.long)
        target = self.loaded_data['y'][index]
        if self.transform:
            sample = self.transform(sample)
        target = torch.tensor(target, dtype=torch.long)

        return sample, target

    def __len__(self):
        return len(self.loaded_data['y'])


class DataSource(base.DataSource):
    def __init__(self, client_id, quiet=False):
        super().__init__(client_id, quiet)
        self.trainset = None
        self.testset = None

        _path = Config().app.data.data_path \
            if hasattr(Config().app.data, "data_path") \
            else "./data"
        root_path = os.path.join(_path, 'Reddit', 'packaged_data')

        data_urls = {}
        if self.client_id == 0:
            # If we are on the federated learning server
            data_dir = os.path.join(root_path, 'test')
            data_urls[self.client_id] = "https://jiangzhifeng.s3.us-east-2.amazonaws.com/Reddit/test/" \
                       + str(self.client_id) + ".zip"
        else:
            data_dir = os.path.join(root_path, 'train')
            data_urls[self.client_id] = "https://jiangzhifeng.s3.us-east-2.amazonaws.com/Reddit/train/" \
                       + str(client_id) + ".zip"

        if hasattr(Config().app.data, "augment") \
                and Config().app.data.augment.type == "simple":
            repeat = Config().app.data.augment.repeat
            total_clients = Config().clients.total_clients
            for i in range(0, repeat):
                client_id = (i + 1) * total_clients + self.client_id
                data_urls[client_id] = "https://jiangzhifeng.s3.us-east-2.amazonaws.com/Reddit/train/" \
                                       + str(client_id) + ".zip"

        for client_id, data_url in data_urls.items():
            if not os.path.exists(os.path.join(data_dir, str(client_id))):
                logging.info(
                    f"Downloading the Reddit dataset for client {client_id}. "
                )
                self.download(url=data_url, data_path=data_dir, quiet=self.quiet)

        loaded_data = {"x": [], "y": []}
        for client_id in data_urls.keys():
            _data = DataSource.read_data(
                file_path=os.path.join(data_dir, str(client_id)))
            loaded_data["x"] += _data["x"]
            loaded_data["y"] += _data["y"]

        logging.info(f"Physical client(s)' data loaded: {list(data_urls.keys())}.")

        # Currently we are using c5.4xlarge as the server in our used cluster
        # because it has no GPUs, evaluation of aggregated models can take
        # a long time period if the size of testing dataset is too large.
        # However, in real environment, it is not uncommon for the server to have GPU.
        # To recover a reasonable server runtime overhead in a used cluster, we thus do a down-sampling.
        # Note that server's testing datasets are IID and we are thus safe to do that.
        if self.client_id == 0 and not torch.cuda.is_available():
            num_reserved_samples = 10000  # TODO: avoid hard-coding
            loaded_data["x"] = loaded_data["x"][:num_reserved_samples]
            loaded_data["y"] = loaded_data["y"][:num_reserved_samples]

        dataset = CustomTextDataset(loaded_data=loaded_data)

        if self.client_id == 0:
            self.testset = dataset
        else:
            self.trainset = dataset

        self.confirm_and_print_partition_size()

    @staticmethod
    def read_data(file_path):
        """ Reading the dataset specific to a client_id. """
        with open(file_path, 'rb') as fin:
            loaded_data = pickle.load(fin)
        return loaded_data
