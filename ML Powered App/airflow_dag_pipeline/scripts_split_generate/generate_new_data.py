import pandas as pd
import numpy as np
import os
np.random.seed(42)
num_rows = 10000
def generate_missing_data(series, missing_rate=0.05):
    mask = np.random.rand(len(series)) < missing_rate
    series[mask] = np.nan
    return series

data = {
    'Loan_ID': ['LP' + str(i).zfill(6) for i in range(num_rows)],
    'Gender': np.random.choice(['Male', 'Female', np.nan], num_rows, p=[0.49, 0.49, 0.02]),
    'Married': np.random.choice(['Yes', 'No', np.nan], num_rows, p=[0.5, 0.49, 0.01]),
    'Dependents': np.random.choice(['0', '1', '2', '3+', np.nan], num_rows, p=[0.57, 0.17, 0.16, 0.09, 0.01]),
    'Education': np.random.choice(['Graduate', 'Not Graduate'], num_rows),
    'Self_Employed': np.random.choice(['Yes', 'No', np.nan], num_rows, p=[0.1, 0.88, 0.02]),
    'ApplicantIncome': np.random.randint(1500, 20000, num_rows),
    'CoapplicantIncome': np.random.randint(0, 10000, num_rows),
    'LoanAmount': np.random.randint(50, 700, num_rows),
    'Loan_Amount_Term': np.random.choice([360, 120, 240, 180, np.nan], num_rows, p=[0.85, 0.05, 0.05, 0.04, 0.01]),
    'Credit_History': np.random.choice([1.0, 0.0, np.nan], num_rows, p=[0.85, 0.14, 0.01]),
    'Property_Area': np.random.choice(['Urban', 'Rural', 'Semiurban'], num_rows),
    'Loan_Status': np.random.choice(['Y', 'N'], num_rows)
}
df = pd.DataFrame(data)
df['ApplicantIncome'] = generate_missing_data(df['ApplicantIncome'], missing_rate=0.02)
df['CoapplicantIncome'] = generate_missing_data(df['CoapplicantIncome'], missing_rate=0.03)
df['LoanAmount'] = generate_missing_data(df['LoanAmount'], missing_rate=0.03)
df['Loan_Amount_Term'] = generate_missing_data(df['Loan_Amount_Term'], missing_rate=0.01)
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
raw_data_dir = os.path.join(parent_dir, "main-data-set")
csv_file_path = os.path.join(raw_data_dir, 'simulated_loan_data.csv')
df.to_csv(csv_file_path, index=False)

print(f"Simulated dataset created with 10k rows and saved as '{csv_file_path}'.")
