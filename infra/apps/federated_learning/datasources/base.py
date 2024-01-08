import os
import sys
import gzip
import tarfile
import zipfile
import logging
import requests
import numpy as np
from infra.config import Config
from urllib.parse import urlparse
from torch.utils.data import Subset
from infra.utils.misc import my_random_zipfian


class DataSource:
    def __init__(self, client_id, quiet=False):
        self.trainset = None
        self.testset = None
        self.client_id = client_id
        self.quiet = quiet
        if not client_id == 0:
            self.num_train_examples = 0

    @staticmethod
    def download(url, data_path, quiet=False):
        """downloads a dataset from a URL."""
        if not os.path.exists(data_path):
            if Config().clients.total_clients > 1:
                if not hasattr(Config().app.data, 'concurrent_download'
                               ) or not Config().app.data.concurrent_download:
                    raise ValueError(
                        "The dataset has not yet been downloaded from the Internet. "
                        "Please re-run with '-d' or '--download' first. ")

            os.makedirs(data_path, exist_ok=True)

        url_parse = urlparse(url)
        file_name = os.path.join(data_path, url_parse.path.split('/')[-1])

        if os.path.exists(file_name.replace('.gz', '')):
            # previously downloaded but not extracted yet
            # redownloading in case of broken file
            os.remove(file_name.replace('.gz', ''))

        if not quiet:
            logging.info("Downloading %s.", url)
        res = requests.get(url, stream=True)
        # is_chunked = res.headers.get('transfer-encoding', '') == 'chunked'
        # content_length_s = res.headers.get('content-length')
        # if not is_chunked and content_length_s.isdigit():
        #     total_size = int(content_length_s)
        # else:
        #     total_size = None
        if "Content-Length" in res.headers:
            total_size = int(res.headers["Content-Length"])
        else:
            total_size = None
        downloaded_size = 0

        with open(file_name, "wb+") as file:
            chunk_cnt = 0
            for chunk in res.iter_content(chunk_size=1024):
                downloaded_size += len(chunk)
                file.write(chunk)
                file.flush()
                if total_size is not None and chunk_cnt % 1024 == 0:
                    sys.stdout.write("\r{:.1f}%".format(100 * downloaded_size /
                                                        total_size))
                    sys.stdout.flush()
                chunk_cnt += 1
            if total_size is not None:
                sys.stdout.write("\n")
                sys.stdout.flush()

        # Unzip the compressed file just downloaded
        if not quiet:
            logging.info("Extracting the dataset.")
        name, suffix = os.path.splitext(file_name)

        if file_name.endswith("tar.gz"):
            tar = tarfile.open(file_name, "r:gz")
            tar.extractall(data_path)
            tar.close()
            os.remove(file_name)
        elif file_name.endswith("tar"):
            tar = tarfile.open(file_name, "r:")
            if not quiet:
                logging.info("Extracting %s to %s.", file_name, data_path)
            tar.extractall(data_path)
            tar.close()
            os.remove(file_name)
        elif suffix == '.zip':
            if not quiet:
                logging.info("Extracting %s to %s.", file_name, data_path)
            with zipfile.ZipFile(file_name, 'r') as zip_ref:
                zip_ref.extractall(data_path)
            os.remove(file_name)
        elif suffix == '.gz':
            unzipped_file = open(name, 'wb')
            zipped_file = gzip.GzipFile(file_name)
            unzipped_file.write(zipped_file.read())
            zipped_file.close()
            os.remove(file_name)
        else:
            logging.info("Unknown compressed file type.")
            sys.exit()

        logging.info("The dataset extracted.")

    def get_num_train_examples(self) -> int:
        return self.num_train_examples

    def get_num_test_examples(self) -> int:
        return len(self.testset)

    def get_num_examples(self):
        if self.client_id == 0:
            return self.get_num_test_examples()
        else:
            return self.get_num_train_examples()

    def confirm_and_print_partition_size(self):
        # first confirm the size
        if not self.client_id == 0:
            if hasattr(Config().app.data, "partition_size"):
                self.num_train_examples = Config().app.data.partition_size
            elif hasattr(Config().app.data, "customized_partition_size"):
                type = Config().app.data.customized_partition_size.type
                if type == "zipf":
                    helping_number = 100000000
                    seed = Config().app.data.customized_partition_size.seed
                    a = Config().app.data.customized_partition_size.a
                    min = Config().app.data.customized_partition_size.min
                    max = Config().app.data.customized_partition_size.max
                    amin = helping_number / max
                    amax = helping_number / min
                    n = Config().clients.total_clients

                    res = my_random_zipfian(
                        a=a, n=n, amin=amin, amax=amax, seed=seed
                    )
                    res = sorted((helping_number / np.array(res))
                                 .astype(int).tolist())
                    self.num_train_examples = res[self.client_id - 1]
                else:
                    raise NotImplementedError

            #     if isinstance(Config().app.data.partition_size, int):
            #         self.num_train_examples = Config().app.data.partition_size
            #     else:  # should be a Config object
            #         type = Config().app.data.customized_partition_size.type
            #         if type == "zipf":
            #             helping_number = 100000000
            #             seed = Config().app.data.customized_partition_size.seed
            #             a = Config().app.data.customized_partition_size.a
            #             min = Config().app.data.customized_partition_size.min
            #             max = Config().app.data.customized_partition_size.max
            #             amin = helping_number / max
            #             amax = helping_number / min
            #             n = Config().clients.total_clients
            #
            #             res = my_random_zipfian(
            #                 a=a, n=n, amin=amin, amax=amax, seed=seed
            #             )
            #             res = sorted((helping_number / np.array(res))
            #                          .astype(int).tolist())
            #             self.num_train_examples = res[self.client_id - 1]
            #         else:
            #             raise NotImplementedError
            else:
                self.num_train_examples = len(self.trainset)
        # here we do not really modify the dataset
        # this is what sampler should be responsible for

        # then print
        if not self.quiet:
            size = self.get_num_examples()
            logging.info(f"Size of data partition: {size}.")

    def classes(self):
        """ Obtains a list of class names in the dataset. """
        return list(self.trainset.classes)

    def targets(self):
        """ Obtains a list of targets (labels) for all the examples
        in the dataset. """
        return self.trainset.targets

    def get_train_set(self):
        """ Obtains the training dataset. """
        return self.trainset

    def get_test_set(self):
        """ Obtains the validation dataset. """
        return self.testset
