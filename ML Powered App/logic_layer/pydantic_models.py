from typing import Optional, List
from pydantic import BaseModel, Field

class Prediction(BaseModel):
    id: int
    result: dict
    prediction: float
    timestamp: str
    
class PredictionRequest(BaseModel):
    ApplicantIncome: float
    CoapplicantIncome: float
    LoanAmount: float
    Loan_Amount_Term: float
    Credit_History: float
    Dependents: str
    Education: str
    Married: str
    Property_Area: str

class PredictionQueryParams(BaseModel):
    start_date: Optional[str] = Field(None, description="Start Date in YYYY-MM-DD format")
    end_date: Optional[str] = Field(None, description="End Date in YYYY-MM-DD format")
    prediction_source: Optional[str] = Field(None, description="Source of prediction")
    page: int = Field(1, description="Page number for pagination", ge=1)
    page_size: int = Field(10, description="Number of items per page", ge=1)

class PredictionResponse(BaseModel):
    data: List[Prediction]
    total_count: int