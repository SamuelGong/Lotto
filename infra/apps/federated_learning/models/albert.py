from transformers import (
    AutoConfig,
    AutoModelForMaskedLM
)


class Model():
    @staticmethod
    def is_valid_model_type(model_type):
        return (model_type.startswith('albert')
                and len(model_type.split('_')) == 3
                and model_type.split('_')[1] in ["base"]
                and model_type.split('_')[2] in ["v2"])

    @staticmethod
    def get_model(model_type, num_classes=None):  # compatibility
        if not Model.is_valid_model_type(model_type):
            raise ValueError(
                'Invalid Albert model type: {}'.format(model_type))

        model_type = model_type.replace('_', '-')
        config = AutoConfig.from_pretrained(model_type)
        model = AutoModelForMaskedLM.from_config(config)
        return model
