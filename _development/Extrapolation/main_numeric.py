import numpy as np

from generate.matrix_history import generate
from extrapolation import extrapolate_matrix_history

np.set_printoptions(linewidth=100000, suppress=True)

data = generate(4, (3, 2))
print(data)
print('––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––')
print(extrapolate_matrix_history(data, 10, 1))