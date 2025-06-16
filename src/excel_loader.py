import pandas as pd

def load_excel(filepath):
    return pd.read_excel(filepath, dtype=str)
