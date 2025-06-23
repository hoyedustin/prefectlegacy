import pandas as pd
import requests
import json
import os
from datetime import datetime

from cryptography.hazmat.backends import default_backend
from datetime import datetime
from prefect import task, flow
from configuration.config import Config
from io import BytesIO
from io import StringIO
from prefect_gcp.cloud_storage import GcsBucket
gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-test-block")

# Task 1: Authentication
@task
def authenticate():
  GetTokenURL = "https://clmapi.landgorilla.com/api/token"
  USER = "removed for security"
  PW = "removed for security"
  body = {'api_name': 'clm'}
  headers = {
    'USER': USER,
    'PASSWORD': PW
    }
  response = requests.get(GetTokenURL, headers=headers, json=body)
  json_data = response.json()
  if response.status_code == 200:
    json_data = response.json()
    api_token = json_data.get('token')
    #test#
    

    if api_token:
        your_variable_name = api_token
        print(f"API Token: {api_token}")
    else:
        print("No API token found in the response JSON.")
  else:
     print("Request was not successful. Status code:", response.status_code)
  return json_data.get('token')


# Task 2: Get most recent ID value in order to pull the most recent report 
@task
def get_most_recent_id_value(api_token):
    ReportURL = f"https://clmapi.landgorilla.com/api/clm/pipelineReport"
    body = {'api_name': 'clm'}
    headers = {
        'Authorization': f'Bearer {api_token}'
        }
        
    ReportResponse = requests.get(ReportURL, headers=headers, json= body)
    print(ReportResponse.text)
    ReportData = ReportResponse.json()
    # Function to convert date string to a comparable format (e.g., MM/DD/YYYY)
    def convert_date(date_string):
        # Assuming the date format is MM/DD/YYYY
        return datetime.strptime(date_string, '%m/%d/%Y')
    
    # Get the dictionary with the most recent reportdate for a specific report name
    report_name = 'Umpqua Portfolio Report'
    
    latest_report = max((data for data in ReportData if data['name'] == report_name), key=lambda x: int(x['id']), default=None)
    latest_id_value = latest_report.get('id')
    return latest_id_value



# Task 3: Pull Report Using Latest ID Value and API Token
@task
def pull_portfolio_report_full(latest_id_value, api_token):
    ActualReportURL = f"https://clmapi.landgorilla.com/api/clm/pipelineReport/{latest_id_value}"
    headers = {
        'Authorization': f'Bearer {api_token}'
        }
    
    ActualReportResponse = requests.get(ActualReportURL, headers=headers)
    ReportData = ActualReportResponse.json()
    data_list = ReportData['data']['Current Report']['data']
    df = pd.DataFrame(data_list)

    today = pd.Timestamp.now(tz='UTC').tz_convert('US/Pacific')
    current_datetime = today.strftime("%Y-%m-%dT%H:%M:%S")
  
    df['timestamp'] = current_datetime
  
    return df



# Task 4: Transform Data
import os, sys
parent_dir = os.path.abspath('..')
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from transform.land_gorilla_umpqua_portfolio_report_transformer import LandGorillaUmpquaPortfolioReportTransformer

@task
def transform_data(df):
    transformer = LandGorillaUmpquaPortfolioReportTransformer(df)
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
  dynamic_filename = f"land_gorilla_umpqua_portfolio_report_{today_date}.csv"
  print (dynamic_filename + "PLACEHOLDER")
  return dynamic_filename


@task
def upload_csv_to_bucket_with_block(gcs_bucket_block_name: str, df: pd.DataFrame, destination_blob_name: str):
  try:
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    gcs_bucket = GcsBucket.load("gcs-test-block")

    gcs_bucket.upload_from_file_object(csv_buffer, destination_blob_name, content_type="text/csv")
    print(f"DataFrame uploaded to GCS bucket as {destination_blob_name} using block {gcs_bucket_block_name}.")
  except Exception as e:
    print(f"Error occurred while uploading file to GCS: {e}")









@flow (log_prints= True)
def UMPQUA_PORTFOLIO_REPORT() -> None:
    api_token = authenticate()
    most_recent_id = get_most_recent_id_value(api_token)
    df = pull_portfolio_report_full(most_recent_id, api_token)
    df_transformed = transform_data(df)
    current_month = pd.Timestamp.now().strftime("%m-%y")
    current_month_folder = f"{current_month}"
    folder = "land-gorilla-umpqua-portfolio-report"
    destination_blob_name = f"{folder}/{current_month_folder}/{get_dynamic_filename()}"
    gcs_bucket_block_name = "gcs-test-block"
    upload_csv_to_bucket_with_block(gcs_bucket_block_name, df_transformed, destination_blob_name)
