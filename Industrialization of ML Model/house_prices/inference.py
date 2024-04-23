import numpy as np
import joblib

MODELS_FOLDER = r'C:\Users\Waleed Satti\dsp-Waleed-Satti\models\\'


def load_models() -> tuple:
    loaded_model = joblib.load(MODELS_FOLDER + 'model.joblib')
    loaded_categorical_encoder = joblib.load(
        MODELS_FOLDER + 'categorical_encoder.joblib'
    )
    loaded_numerical_scaler = joblib.load(
        MODELS_FOLDER + 'numerical_scaler.joblib'
    )
    return loaded_model, loaded_categorical_encoder, loaded_numerical_scaler


def preprocess_data(user_data_df, loaded_categorical_encoder,
                    loaded_numerical_scaler) -> np.ndarray:
    categorical_columns = ['LotConfig', 'LotShape']
    numerical_columns = ['LotArea', 'MSSubClass']
    dataset_test_encoder_numeric = loaded_numerical_scaler.transform(
        user_data_df[numerical_columns]
    )
    dataset_test_encoder_categorical = loaded_categorical_encoder.transform(
        user_data_df[categorical_columns]
    )
    final_test = np.concatenate([
        dataset_test_encoder_numeric,
        dataset_test_encoder_categorical.toarray()
    ], axis=1)
    return final_test


def make_predictions(user_data_df) -> np.ndarray:
    loaded_model, loaded_categorical_encoder, loaded_numerical_scaler = \
        load_models()
    final_test = preprocess_data(user_data_df,
                                 loaded_categorical_encoder,
                                 loaded_numerical_scaler)
    predictions = loaded_model.predict(final_test)
    return predictions
