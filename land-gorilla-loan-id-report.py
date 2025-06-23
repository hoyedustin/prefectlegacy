import pandas as pd
import requests
import json
import os
from datetime import datetime
from google.cloud import storage
from cryptography.hazmat.backends import default_backend
from prefect import task, flow
from configuration.config import Config
from io import BytesIO
from io import StringIO
from prefect_gcp.cloud_storage import GcsBucket
gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-test-block")

# Define Prefect Task to get API Token
@task
def get_api_token():
    GetTokenURL = "https://clmapi.landgorilla.com/api/token"
    
    body = {'api_name': 'clm'}
    
    headers = {
        'USER': 'removed for security',
        'PASSWORD': 'removed for security'
    }
    
    response = requests.get(GetTokenURL, headers=headers, json=body)
    json_data = response.json()
    api_token = json_data.get('token')

    if not api_token:
        raise ValueError("Failed to retrieve API token.")
    
    return api_token

# Define Prefect Task to fetch loan data
@task
def fetch_loan_data(api_token):
    url = "https://clmapi.landgorilla.com/api/clm/loan?page=1&perPage=1000&folder[]=active&status[]=active"  # Reduced perPage
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers, json={})
    json_response = response.json()

    loan_list = json_response.get("data", {})
    loan_specific = loan_list.get("items", [])

    return loan_specific

# Define Prefect Task to process loan data
@task
def process_loan_data(loan_specific):
    rows = [
        {
            "Land Gorilla Loan ID": loan.get("id"),
            "Loan Number": loan.get("fileNumber"),
            "First Name": loan.get("borrower", {}).get("firstName", ""),
            "Last Name / Business": loan.get("borrower", {}).get("lastName", "")
        }
        for loan in loan_specific
    ]

    df = pd.DataFrame(rows)

    #Adding a timestamp columns
    today = pd.Timestamp.now(tz='UTC').tz_convert('US/Pacific')
    current_datetime = today.strftime("%Y-%m-%dT%H:%M:%S")
    df['timestamp'] = current_datetime
    
    return df

# Task 4: Transform Data
import os, sys
parent_dir = os.path.abspath('..')
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from transform.land_gorilla_loan_id_report_transformer import LandGorillaLoanIDTransformer

@task
def transform_data(df):
    transformer = LandGorillaLoanIDTransformer(df)
    transformer.transform_data()
    return transformer.df



@task
def get_dynamic_filename():
  
  
  ## Generate a dynamic filename based on the current date. ##
  ## A string representing the filename with a date-based naming convention. ##
  today = pd.Timestamp.now().normalize()
  today_date = today.strftime("%m%d%y")  # MMDDYY format
  today_date_short = today.strftime("%m%y")  # MMYY format
  
  ## Create a dynamic naming convention using the date ##
  dynamic_filename = f"land_gorilla_loan_id_report_{today_date}.csv"
  print (dynamic_filename + "PLACEHOLDER")
  return dynamic_filename

@task
def upload_csv_to_bucket_with_block(gcs_bucket_block_name: str, df: pd.DataFrame, destination_blob_name: str):
  try:
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
 
    gcs_bucket = GcsBucket.load("gcs-test-block")
 
    gcs_bucket.upload_from_file_object(csv_buffer, destination_blob_name)
    print(f"DataFrame uploaded to GCS bucket as {destination_blob_name} using block {gcs_bucket_block_name}.")
  except Exception as e:
    print(f"Error occurred while uploading file to GCS: {e}")

# Define Prefect Flow
@flow
def LOAN_ID_REPORT():
    api_token = get_api_token()
    loan_data = fetch_loan_data(api_token)
    df = process_loan_data(loan_data)
    df = transform_data(df)
    current_month = pd.Timestamp.now().strftime("%m-%y")
    current_month_folder = f"{current_month}"
    folder = "land-gorilla-loan-id-report"
    destination_blob_name = f"{folder}/{current_month_folder}/{get_dynamic_filename()}"
    gcs_bucket_block_name = "gcs-test-block"
    upload_csv_to_bucket_with_block(gcs_bucket_block_name, df, destination_blob_name)


