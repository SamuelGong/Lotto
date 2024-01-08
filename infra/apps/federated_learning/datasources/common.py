import logging

from torch.utils.data import Dataset


class CustomDictDataset(Dataset):
    def __init__(self, loaded_data, transform=None):
        super().__init__()
        self.loaded_data = loaded_data
        self.transform = transform

    def __getitem__(self, index):
        sample = self.loaded_data['x'][index]
        target = self.loaded_data['y'][index]
        if self.transform:
            sample = self.transform(sample)

        return sample, target

    def __len__(self):
        return len(self.loaded_data['y'])
