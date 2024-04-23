from io import StringIO
import json
import streamlit as st
import pandas as pd
import os
import requests
from tempfile import NamedTemporaryFile

BASE_URL = os.getenv('BASE_URL')

st.set_page_config(
    page_title='Loan Eligibility Prediction',
    page_icon='🏠',
    layout='wide', 
    initial_sidebar_state='auto' 
)

def prediction_page():
    st.write("House Loan Prediction Page")

    with st.form(key='prediction_form'):
        applicant_income = st.number_input('Applicant Income', min_value=0.0, step=100.0, format="%.2f")
        coapplicant_income = st.number_input('Coapplicant Income', min_value=0.0, value=0.0, step=100.0, format="%.2f")
        loan_amount = st.number_input('Loan Amount', min_value=0.0, step=1000.0, format="%.3f")
        loan_amount_term = st.number_input('Loan Amount Term', min_value=0.0, step=1.0, format="%.1f")

        credit_history = st.radio('Credit History', options=['Yes', 'No'])
        dependents = st.selectbox('Dependents', options=['0','1','2','3+'])
        education = st.radio('Education', options=['Graduate', 'Not Graduate'])
        married = st.radio('Married', options=['Yes', 'No'])
        property_area = st.selectbox('Property Area', options=['Rural', 'Semiurban', 'Urban'])
       
        get_prediction = st.form_submit_button(label='Check Eligibility')

    uploaded_file = st.file_uploader("Upload CSV file for multiple predictions", type=['csv'])

    if uploaded_file is not None and uploaded_file.size > 200 * 1024 * 1024:
        st.error("File size exceeds the maximum limit of 200MB. Please upload a smaller file.")
        return

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write(df.head(25))
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  
        files = {'file': ('dataframe.csv', csv_buffer.getvalue(), 'text/csv')}

        response = requests.post(BASE_URL + '/file/upload', files=files)

        if response.ok:
            st.success("File successfully uploaded and is eligible for predictions")
            if st.button("Predict from CSV"):
                predict_response = requests.post(BASE_URL + '/predict_from_csv',files=files)
                if predict_response.ok:
                    st.success("Prediction successful")
                    st.table(pd.DataFrame(predict_response.json()))
                else:
                    st.error("Failed to make predictions.")
        else:
            st.error("File structure not compatible with the model. Please upload a valid file.")
            response_json = json.loads(response.text)
            detail_message = response_json.get("detail")
            st.info(detail_message)

    if get_prediction:
        form_data = {
            'ApplicantIncome': applicant_income,
            'CoapplicantIncome': coapplicant_income,
            'LoanAmount': loan_amount,
            'Loan_Amount_Term': loan_amount_term,
            'Credit_History': 1 if credit_history == 'Yes' else 0,
            'Dependents': dependents,
            'Education': education,
            'Married': married,
            'Property_Area': property_area
        }

        response = requests.post(BASE_URL + '/predict', json=form_data)
        if response.status_code == 200:
            predict_response = response.json()
            features = predict_response["response_item"]
            features['Prediction'] = predict_response["prediction"]
            st.success('Prediction successful information below:')
            result_df = pd.DataFrame([features])
            st.table(result_df)
        else:
            st.error('Failed to get prediction from the API.')

def past_predictions_page():
    st.title("Past Predictions")
    start_date = st.date_input("Start Date", value=None)
    end_date = st.date_input("End Date", value=None)
    prediction_source = st.selectbox("Prediction Source", options=["webapp", "csv-predictions", "scheduled predictions", "all"])

    page = st.number_input("Page Number", min_value=1, value=1, step=1)
    page_size = st.selectbox("Predictions per Page", options=[10, 20, 50, 100], index=0)

    query_params = {
        "start_date": start_date.strftime('%Y-%m-%d') if start_date else None,
        "end_date": end_date.strftime('%Y-%m-%d') if end_date else None,
        "prediction_source": prediction_source,
        "page": page,
        "page_size": page_size
    }

    get_previous_predictions = st.button(label='Get Past Predictions')

    if get_previous_predictions:
        try:
            response = requests.get(BASE_URL + '/get-past-predictions', params=query_params)
            response.raise_for_status()
            response_data = response.json()

            predictions_data = pd.DataFrame(response_data["data"])
            total_count = response_data["total_count"]
            total_pages = (total_count + page_size - 1) // page_size

            if not predictions_data.empty:
                st.table(predictions_data[['id', 'prediction', 'timestamp']])
                st.write("Detailed JSON Data:")
                for index, row in predictions_data.iterrows():
                    with st.expander(f"Details for Prediction ID {row['id']}"):
                        st.json(row['result'])
            else:
                st.warning("No prediction data received from the API.")

            col1, col2 = st.columns(2)
            with col1:
                if page > 1:
                    prev_page = st.button("Previous", key='prev')
                    if prev_page:
                        page -= 1
                        st.experimental_rerun()

            with col2:
                if page < total_pages:
                    next_page = st.button("Next", key='next')
                    if next_page:
                        page += 1
                        st.experimental_rerun()

        except requests.exceptions.RequestException as e:
            st.error(f"Error retrieving past predictions: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")


def main():
    st.title("Loan Eligibility Prediction 🏠")

    page = st.sidebar.selectbox("Select Page", ["Prediction", "Past Predictions"])

    if page == "Prediction":
        prediction_page()
    elif page == "Past Predictions":
        past_predictions_page()

if __name__ == "__main__":
    main()