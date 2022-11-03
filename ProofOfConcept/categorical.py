import numpy as np
import pandas as pd
from test import CarData

import pandas_utilities as pdu




  # TODO to numpy?






bcm = find_categorical_correlations(CarData)
encoded = encode_categorical_correlations(CarData, bcm)
print(encoded)
decoded = decode_categorical_correlations(encoded, bcm)
print(decoded)





