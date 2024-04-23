import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.linear_model import LinearRegression
from .preprocess import preprocess_data
import joblib


def build_model(training_data_df) -> float:
    """
    Build a linear regression model and return the RMSE.
    """
    (X_test_final, X_train, y_train,
     y_test, numeric_transformer,
     categorical_transformer) = preprocess_data(training_data_df)
    model = LinearRegression()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test_final)
    taking_dump(model, categorical_transformer, numeric_transformer)
    # Calculate RMSE
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    return rmse


def taking_dump(model, categorical_transformer, numeric_transformer) -> None:
    """
    Dump the trained model and transformers to disk.
    """
    models_folder = (
        r'C:\Users\Waleed Satti\dsp-Waleed-Satti\models\\'
    )
    joblib.dump(model, models_folder + 'model.joblib')
    joblib.dump(categorical_transformer,
                models_folder + 'categorical_encoder.joblib')
    joblib.dump(numeric_transformer,
                models_folder + 'numerical_scaler.joblib')
