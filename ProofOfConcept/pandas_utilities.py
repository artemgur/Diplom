import pandas as pd


def get_categorical_columns(data: pd.DataFrame) -> pd.DataFrame:
    return data.select_dtypes(['object'])


def get_numerical_columns(data: pd.DataFrame) -> pd.DataFrame:
    return data.select_dtypes(exclude=['object'])


def get_categorical_column_names(data: pd.DataFrame) -> list[str]:
    return list(get_categorical_columns(data).columns)

def get_numerical_column_names(data: pd.DataFrame) -> list[str]:
    return list(get_categorical_columns(data).columns)
