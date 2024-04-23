import asyncio
import subprocess
from dotenv import load_dotenv
from logic_layer.postgres_database.database_app import initialize_db

def run_fastapi():
    return ["uvicorn", "api_layer.fast_api.fast_api_app:app", "--reload"]

def run_streamlit():
    return ["streamlit", "run", "application_layer/streamlit_front_end/streamlit_app.py"]

if __name__ == "__main__":
    load_dotenv()
    asyncio.run(initialize_db())

    fastapi_process = subprocess.Popen(run_fastapi(), shell=False)
    streamlit_process = subprocess.Popen(run_streamlit(), shell=False)
    
    fastapi_process.wait()
    streamlit_process.wait()
