from extrapolation import extrapolate_categorical_matrix
from generate.matrix import categorical_df


data = categorical_df((10, 4))
print(data)
print('––––––––––––––––––––––––––––––––––––––––––––')
extrapolated = extrapolate_categorical_matrix(data, 10, 1)
print(extrapolated)
