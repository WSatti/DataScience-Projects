import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from typing import Tuple
from acceptance_prediction.constants import (CONTINUOUS_FEATURES,
                                    CATEGORICAL_FEATURES,
                                    TARGET_FEATURE)


def prepare_data(data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    X = data[CONTINUOUS_FEATURES + CATEGORICAL_FEATURES]
    y = data[TARGET_FEATURE]
    
    label_encoder = LabelEncoder()
    y_encoded = label_encoder.fit_transform(y)
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y_encoded, test_size=0.3, random_state=42)
    return X_train, X_test, y_train, y_test


def preprocess_data(X_train: pd.DataFrame, X_test: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    scaler = StandardScaler()
    encoder = OneHotEncoder(handle_unknown='ignore')
    continuous_imputer = SimpleImputer(strategy='mean')
    categorical_imputer = SimpleImputer(strategy='most_frequent')

    X_train_continuous = continuous_imputer.fit_transform(X_train[CONTINUOUS_FEATURES])
    X_test_continuous = continuous_imputer.transform(X_test[CONTINUOUS_FEATURES])

    X_train_continuous_scaled = scaler.fit_transform(X_train_continuous)
    X_test_continuous_scaled = scaler.transform(X_test_continuous)

    X_train_categorical = categorical_imputer.fit_transform(X_train[CATEGORICAL_FEATURES])
    X_test_categorical = categorical_imputer.transform(X_test[CATEGORICAL_FEATURES])

    X_train_categorical_encoded = encoder.fit_transform(X_train_categorical).toarray()
    X_test_categorical_encoded = encoder.transform(X_test_categorical).toarray()

    X_train_preprocessed = np.concatenate([X_train_continuous_scaled, X_train_categorical_encoded], axis=1)
    X_test_preprocessed = np.concatenate([X_test_continuous_scaled, X_test_categorical_encoded], axis=1)

    return X_train_preprocessed, X_test_preprocessed, scaler, encoder, categorical_imputer, continuous_imputer