import json
from fastapi import FastAPI, UploadFile, File, Depends
from typing import List, Dict, Union
import pandas as pd

from api_layer.fast_api.service_slices import fetch_past_predictions, handle_csv_file, insert_inference_data
from logic_layer.acceptance_prediction.csv_service import validate_csv
from logic_layer.acceptance_prediction.inference import PredictionModel
from logic_layer.pydantic_models import Prediction, PredictionQueryParams, PredictionRequest, PredictionResponse

app = FastAPI()

predictor = PredictionModel()

async def get_predictor():
    return predictor

@app.post("/predict")
async def predict(request_data: Union[PredictionRequest, Dict], predictor: PredictionModel = Depends(get_predictor)):
    input_dict = request_data.model_dump() if isinstance(request_data, PredictionRequest) else request_data
    prediction = predictor.make_prediction(input_dict)
    await insert_inference_data(input_dict, prediction.tolist()[0], 'webapp')
    eligibility = prediction.tolist()[0] == 1 and 'Eligible' or 'Not Eligible'
    return {"response_item": request_data, "prediction": eligibility}

@app.get("/get-past-predictions", response_model=PredictionResponse)
async def get_past_predictions_endpoint(params: PredictionQueryParams = Depends()):
    predictions = await fetch_past_predictions(params)
    prediction_list = [
        Prediction(
            id=prediction['id'],
            result=json.loads(prediction.get('request_data', '{}')),
            prediction=str(prediction['prediction']),
            timestamp=str(prediction['timestamp'])
        ) for prediction in predictions
    ]
    return PredictionResponse(data=prediction_list, total_count=len(prediction_list))

@app.post("/file/upload")
async def create_upload_file(file: UploadFile = File(...)):
    file_in_memory = await handle_csv_file(file)
    is_valid, message = validate_csv(file_in_memory)
    return {"filename": file.filename, "is_valid": is_valid, "message": message}

@app.post("/predict_from_csv")
async def predict_from_csv(file: UploadFile = File(...)):
    file_in_memory = await handle_csv_file(file)
    df = pd.read_csv(file_in_memory)
    predictor = await get_predictor()
    response_list = []

    for item in predictor.make_predictions_from_csv(df):
            features = item['features']
            prediction = item['prediction']
            response_item = {
                "Credit_History": features['Credit_History'],
                "Dependents": features['Dependents'],
                "Education": features['Education'],
                "Married": features['Married'],
                "Property_Area": features['Property_Area'],
                "ApplicantIncome": features['ApplicantIncome'],
                "CoapplicantIncome": features['CoapplicantIncome'],
                "LoanAmount": features['LoanAmount'],
                "Loan_Amount_Term": features['Loan_Amount_Term'],
            }
            await insert_inference_data(response_item, prediction, 'csv-predictions')
            response_item['prediction'] = prediction == 1 and 'Eligible' or 'Not Eligible'
            response_list.append(response_item)

    return response_list
