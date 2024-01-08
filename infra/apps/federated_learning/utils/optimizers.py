from torch import optim
from torch import nn
import numpy as np
import bisect
from infra.apps.federated_learning.utils.step import Step
from infra.config import Config


def get_optimizer(model) -> optim.Optimizer:
    weight_decay = Config().app.trainer.weight_decay
    model_name = Config().app.trainer.model_name
    if 'albert' in model_name:
        no_decay = ["bias", "LayerNorm.weight"]
    else:
        no_decay = []
    optimizer_grouped_parameters = [
        {
            "params": [p for n, p in model.named_parameters()
                       if not any(nd in n for nd in no_decay)],
            "weight_decay": weight_decay,
        },
        {
            "params": [p for n, p in model.named_parameters()
                       if any(nd in n for nd in no_decay)],
            "weight_decay": 0.0,
        },
    ]

    if Config().app.trainer.optimizer == 'SGD':
        return optim.SGD(optimizer_grouped_parameters,
                         lr=Config().app.trainer.learning_rate,
                         momentum=Config().app.trainer.momentum)
    elif Config().app.trainer.optimizer == 'Adam':
        return optim.Adam(optimizer_grouped_parameters,
                          lr=Config().app.trainer.learning_rate)
    elif Config().app.trainer.optimizer == 'AdamW':
        return optim.AdamW(optimizer_grouped_parameters,
                           lr=Config().app.trainer.learning_rate)

    raise ValueError('No such FL optimizer: {}'.format(
        Config().app.trainer.optimizer))


def get_lr_schedule(optimizer: optim.Optimizer,
                    iterations_per_epoch: int,
                    train_loader=None):
    """Returns a learning rate scheduler according to the configuration."""
    if Config().trainer.lr_schedule == 'CosineAnnealingLR':
        return optim.lr_scheduler.CosineAnnealingLR(
            optimizer,
            len(train_loader) * Config().trainer.epochs)
    elif Config().trainer.lr_schedule == 'LambdaLR':
        lambdas = [lambda it: 1.0]

        if hasattr(Config().trainer, 'lr_gamma') and hasattr(
                Config().trainer, 'lr_milestone_steps'):
            milestones = [
                Step.from_str(x, iterations_per_epoch).iteration
                for x in Config().trainer.lr_milestone_steps.split(',')
            ]
            lambdas.append(lambda it: Config().trainer.lr_gamma**bisect.bisect(
                milestones, it))

        # Add a linear learning rate warmup if specified
        if hasattr(Config().trainer, 'lr_warmup_steps'):
            warmup_iters = Step.from_str(Config().trainer.lr_warmup_steps,
                                         iterations_per_epoch).iteration
            lambdas.append(lambda it: min(1.0, it / warmup_iters))

        # Combine the lambdas
        return optim.lr_scheduler.LambdaLR(
            optimizer, lambda it: np.product([l(it) for l in lambdas]))
    elif Config().trainer.lr_schedule == 'StepLR':
        step_size = Config().trainer.lr_step_size if hasattr(
            Config().trainer, 'lr_step_size') else 30
        gamma = Config().trainer.lr_gamma if hasattr(Config().trainer,
                                                     'lr_gamma') else 0.1
        return optim.lr_scheduler.StepLR(optimizer,
                                         step_size=step_size,
                                         gamma=gamma)
    elif Config().trainer.lr_schedule == 'ReduceLROnPlateau':
        factor = Config().trainer.lr_factor if hasattr(Config().trainer,
                                                       'lr_factor') else 0.1
        patience = Config().trainer.lr_patience if hasattr(
            Config().trainer, 'lr_patience') else 10
        return optim.lr_scheduler.ReduceLROnPlateau(optimizer,
                                                    mode='min',
                                                    factor=factor,
                                                    patience=patience)
    else:
        raise ValueError('Unknown learning rate scheduler.')


def get_loss_criterion():
    if hasattr(Config().app.trainer, "loss_criterion") \
            and Config().app.trainer.loss_criterion == 'BCEWithLogitsLoss':
        return nn.BCEWithLogitsLoss()
    else:
        return nn.CrossEntropyLoss()
