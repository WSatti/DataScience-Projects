from typing import Tuple
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler


class CustomPreprocessor:
    def __init__(self):
        self.numeric_transformer = StandardScaler()
        self.categorical_transformer = OneHotEncoder(handle_unknown='ignore')

    def fit(self, X_train_raw) -> None:
        numerical_columns = ['LotArea', 'MSSubClass']
        categorical_columns = ['LotConfig', 'LotShape']

        self.numeric_transformer.fit(X_train_raw[numerical_columns])
        self.categorical_transformer.fit(X_train_raw[categorical_columns])

    def transform(self, X_raw) -> np.ndarray:
        numerical_columns = ['LotArea', 'MSSubClass']
        categorical_columns = ['LotConfig', 'LotShape']

        X_transformed = np.concatenate([
            self.numeric_transformer.transform(X_raw[numerical_columns]),
            self.categorical_transformer
            .transform(X_raw[categorical_columns]).toarray()
        ], axis=1)
        return X_transformed


def preprocess_data(training_data_df) -> Tuple[
    np.ndarray, np.ndarray, np.ndarray, np.ndarray,
    StandardScaler, OneHotEncoder
]:
    X = training_data_df[['LotArea', 'MSSubClass', 'LotConfig', 'LotShape']]
    y = training_data_df['SalePrice']
    X_train_raw, X_test_raw, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=1
    )

    preprocessor = CustomPreprocessor()
    preprocessor.fit(X_train_raw)

    X_train = preprocessor.transform(X_train_raw)
    X_test = preprocessor.transform(X_test_raw)

    return (
        X_test,
        X_train,
        y_train,
        y_test,
        preprocessor.numeric_transformer,
        preprocessor.categorical_transformer
    )
