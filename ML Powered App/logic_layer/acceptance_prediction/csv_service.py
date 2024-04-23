import pandas as pd
from .constants import CONTINUOUS_FEATURES, CATEGORICAL_FEATURES

expected_types = {
    "continuous": ["int64", "float64"],
    "categorical": ["object", "category", "float64"]
}

def validate_csv(file):
    try:
        df = pd.read_csv(file)
    except Exception as e:
        return False, f"Error reading CSV file: {e}"

    error_messages = []

    missing_continuous = [feature for feature in CONTINUOUS_FEATURES if feature not in df.columns]
    if missing_continuous:
        error_messages.append(f"Missing continuous feature(s): {', '.join(missing_continuous)}")

    missing_categorical = [feature for feature in CATEGORICAL_FEATURES if feature not in df.columns]
    if missing_categorical:
        error_messages.append(f"Missing categorical feature(s): {', '.join(missing_categorical)}")

    for feature in CONTINUOUS_FEATURES:
        if feature in df.columns and df[feature].dtype not in expected_types["continuous"]:
            error_messages.append(f"Continuous feature '{feature}' has incorrect type '{df[feature].dtype}'")

    for feature in CATEGORICAL_FEATURES:
        if feature in df.columns and df[feature].dtype.name not in expected_types["categorical"]:
            error_messages.append(f"Categorical feature '{feature}' has incorrect type '{df[feature].dtype}'")

    if error_messages:
        formatted_errors = "\n- ".join(error_messages)
        return False, "Validation failed with the following errors:\n- " + formatted_errors

    return True, "CSV file is valid."