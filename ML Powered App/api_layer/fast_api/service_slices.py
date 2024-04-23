import json
from datetime import datetime
from io import BytesIO
from fastapi import HTTPException, UploadFile

from logic_layer.acceptance_prediction.csv_service import validate_csv
from logic_layer.postgres_database.database_utils import create_db_connection
from logic_layer.pydantic_models import PredictionQueryParams

async def fetch_past_predictions(params: PredictionQueryParams):
    offset = (params.page - 1) * params.page_size
    conn = await create_db_connection()

    query = 'SELECT * FROM api_inferences'
    conditions = []
    query_params = []

    if params.start_date:
        start_date_obj = datetime.strptime(params.start_date, '%Y-%m-%d').date()
        conditions.append(f"timestamp >= $1")
        query_params.append(start_date_obj)
    if params.end_date:
        end_date_obj = datetime.strptime(params.end_date, '%Y-%m-%d').date()
        conditions.append(f"timestamp <= ${len(query_params) + 1}")
        query_params.append(end_date_obj)

    if params.prediction_source and params.prediction_source.lower() != 'all':
        conditions.append(f"prediction_source = ${len(query_params) + 1}")
        query_params.append(params.prediction_source)

    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)

    query += f' ORDER BY timestamp DESC LIMIT ${len(query_params) + 1} OFFSET ${len(query_params) + 2}'

    query_params += [params.page_size, offset]

    rows = await conn.fetch(query, *query_params)
    await conn.close()
    return rows

async def insert_inference_data(data_input, prediction, prediction_source):
    conn = await create_db_connection()
    async with conn.transaction():
        await conn.execute('''
            INSERT INTO api_inferences (request_data, prediction, prediction_source) VALUES ($1, $2, $3)
        ''', json.dumps(data_input), str(prediction), str(prediction_source))
    await conn.close()

async def handle_csv_file(file: UploadFile):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be CSV")

    file_content = await file.read()
    MAX_FILE_SIZE_BYTES = 200 * 1024 * 1024

    if len(file_content) > MAX_FILE_SIZE_BYTES:
        raise HTTPException(status_code=413, detail="File size exceeds maximum limit")

    file_in_memory = BytesIO(file_content)
    is_valid, message = validate_csv(file_in_memory)
    if not is_valid:
        raise HTTPException(status_code=400, detail=message)
    
    file_in_memory.seek(0)
    return file_in_memory