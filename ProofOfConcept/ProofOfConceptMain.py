import numpy as np

from generate_data import generate
from extrapolation import extrapolate


np.set_printoptions(linewidth=100000, suppress=True)

data, description = generate(4, 3, 2, 10)
print(data)
print('––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––')
print(extrapolate(data, 10, 1))