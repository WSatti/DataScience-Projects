from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import random
import shutil

def read_data():
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    raw_data_folder = os.path.join(dag_folder, 'raw-data')
    if os.path.exists(raw_data_folder):
        files = os.listdir(raw_data_folder)
        if files:
            file_name = random.choice(files)
            file_path = os.path.join(raw_data_folder, file_name)
            print("Selected file:", file_name)
            return file_path
        else:
            print("No files available in raw-data folder.")
            return None
    else:
        print("The raw-data folder does not exist.")
        return None
def save_file(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='read_data')
    print("Received file path:", file_path)
    if file_path:
        if os.path.exists(file_path): 
            good_data_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "good-data") 
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(good_data_folder, file_name)
            if os.path.exists(destination_path):
                base_name, ext = os.path.splitext(file_name)
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                new_file_name = f"{base_name}_{timestamp}{ext}"
                destination_path = os.path.join(good_data_folder, new_file_name)
                print(f"File '{file_name}' already exists in 'good-data' folder. Renaming to '{new_file_name}'.")
            shutil.move(file_path, destination_path)
            print(f"File '{file_name}' moved to 'good-data' folder.")
            return file_path
        else:
            print(f"File '{file_path}' does not exist. Skipping save_file task.")
            return None
    else:
        print("No files available in raw-data folder. Skipping save_file task.")
        return None

default_args = {
   'owner': 'dsp_airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

with DAG('data_ingestion_pipeline', default_args=default_args, schedule_interval='*/1 * * * *') as dag:
    read_data_task = PythonOperator(
        task_id='read_data',
        python_callable=read_data
    )

    save_file_task = PythonOperator(
    task_id='save_file',
    python_callable=save_file,
    provide_context=True
    )

    read_data_task >> save_file_task  
