import numpy as np

def count_params(model):
    total_params = 0

    for x in filter(lambda p: p.requires_grad, model.parameters()):
        total_params += np.prod(x.data.numpy().shape)
    return total_params
