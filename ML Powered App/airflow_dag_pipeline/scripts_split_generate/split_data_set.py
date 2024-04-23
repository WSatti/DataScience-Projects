import pandas as pd
import os
import numpy as np

def main():
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_dir = os.path.join(parent_dir, "main-data-set")
    if not os.path.exists(input_dir):
        print("Input directory not found:", input_dir)
        return
    csv_files = [file for file in os.listdir(input_dir) if file.endswith('.csv')]
    if not csv_files:
        print("No CSV files found in the directory:", input_dir)
        return
    else:
        print("CSV files found:", csv_files)

    for csv_file in csv_files:
        csv_file_path = os.path.join(input_dir, csv_file)
        print(f"Processing {csv_file}...")
        df = pd.read_csv(csv_file_path)
        total_rows = len(df)
        print(total_rows)
        rows_per_file = total_rows // 20  
        output_dir = os.path.join(parent_dir, "dags", "raw-data")
        os.makedirs(output_dir, exist_ok=True)
        for i, chunk in enumerate(np.array_split(df, 20)):
            output_file_path = os.path.join(output_dir, f"{os.path.splitext(csv_file)[0]}_{i+1}.csv")
            chunk.to_csv(output_file_path, index=False)
            print(f"Chunk {i+1} saved to {output_file_path}")

if __name__ == '__main__':
    main()