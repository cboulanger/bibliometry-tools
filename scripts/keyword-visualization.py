import sys, os
import pandas as pd

pickle_path = sys.argv[1]
if not os.path.isfile(pickle_path):
    raise FileNotFoundError(f"No or invalid path to pickle file")
df = pd.read_pickle(pickle_path)

