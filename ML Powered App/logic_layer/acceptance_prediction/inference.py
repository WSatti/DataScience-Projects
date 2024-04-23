import os
import numpy as np
import pandas as pd
from joblib import load
from .constants import CONTINUOUS_FEATURES, CATEGORICAL_FEATURES

class PredictionModel:
    def __init__(self):
        self.scaler = load(os.path.join(os.getcwd(), 'logic_layer/model/scaler.joblib'))
        self.encoder = load(os.path.join(os.getcwd(), 'logic_layer/model/encoder.joblib'))
        self.model = load(os.path.join(os.getcwd(), 'logic_layer/model/model.joblib'))
        self.cont_imputer = load(os.path.join(os.getcwd(), 'logic_layer/model/continuous_imputer.joblib'))
        self.cat_imputer = load(os.path.join(os.getcwd(), 'logic_layer/model/categorical_imputer.joblib'))

    def make_prediction(self, input_dict: dict):
        continuous_features = np.array([[input_dict[feature] for feature in CONTINUOUS_FEATURES]])
        categorical_features = np.array([[input_dict[feature] for feature in CATEGORICAL_FEATURES]])
        categorical_features_encoded = self.encoder.transform(categorical_features).toarray()
        continuous_features_scaled = self.scaler.transform(continuous_features)

        input_vector_processed = np.concatenate([continuous_features_scaled, categorical_features_encoded], axis=1)
        prediction = self.model.predict(input_vector_processed)
        
        return prediction

    def make_predictions_from_csv(self, data_frame: pd.DataFrame):
        df = data_frame[CATEGORICAL_FEATURES + CONTINUOUS_FEATURES]
        df[CONTINUOUS_FEATURES] = self.cont_imputer.transform(df[CONTINUOUS_FEATURES])
        df[CATEGORICAL_FEATURES] = self.cat_imputer.transform(df[CATEGORICAL_FEATURES])

        predictions = []

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            prediction = self.make_prediction(row_dict)
            predictions.append({"features" : row_dict, "prediction": prediction.tolist()[0]})

        return predictions