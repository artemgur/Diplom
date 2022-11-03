import numpy as np
import pandas as pd

from generate_matrix_history import generate
from extrapolation import extrapolate_matrix


np.set_printoptions(linewidth=100000, suppress=True)

data = generate(4, 3, 2, 10)
print(pd.DataFrame(data))
#print(data)
#print('––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––')
#print(extrapolate(data, 10, 1))