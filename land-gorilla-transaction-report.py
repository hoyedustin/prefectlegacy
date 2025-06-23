import snowflake.connector
import requests
import base64
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from prefect import task, flow
from postmarker.core import PostmarkClient
from prefect.blocks.system import Secret
from configuration.config import Config
from io import BytesIO
from io import StringIO
from prefect_gcp.cloud_storage import GcsBucket
gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-test-block")
 
postmark_server_token = Secret.load("postmark-token")
 
pd.reset_option('display.max_rows')
 
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
   
   
    json_data = response.json()
    api_token = json_data.get('token')
 
    return api_token
 
@task
def GetReportData(api_token):
 
    list_accounts_url ="https://clmapi.landgorilla.com/api/clm/GLAccounts"
 
    body = {'api_name': 'clm'}
    headers = {
        'Authorization': f'Bearer {api_token}'
        }
   
    ReportResponse = requests.get(list_accounts_url, headers=headers, json= body)
    reportdata = ReportResponse.json()
    transactions = reportdata.get("data", {}).get("items", [])
   
   
    df = pd.DataFrame(transactions)
   
    expected_columns = ["id"]
   
    id_values = df[[col for col in expected_columns if col in df.columns]]
 
    return id_values
 
@task
def GetTransactions (id_values, api_token):
 
    today = pd.Timestamp.today()
 
    two_months_back = today - pd.DateOffset(months=2)
   
    today_str = today.strftime("%m/%d/%Y")
   
    two_months_back_str = two_months_back.strftime("%m/%d/%Y")
   
    all_transactions = []
   
    for account_id in id_values["id"].dropna().astype(str):
        transactionreportURL = f"https://clmapi.landgorilla.com/api/clm/transactionReport?listAccounts={account_id}&reportPeriod=custom&rangeDate={two_months_back_str}-{today_str}"
 
        body = {'api_name': 'clm'}
        headers = {
            'Authorization': f'Bearer {api_token}'
            }
   
        response = requests.get(transactionreportURL, headers=headers, json=body)
        report_data = response.json()
   
        transactions = report_data.get("data", {}).get("items", [])
        all_transactions.extend(transactions)
   
    expected_columns = [
        "loanNumber",
        "transactionDescription",
        "debit",
        "credit",
        "paymentType",
        "transactionCreatedDate",
        "transactionEffectiveDate"
   
    ]
   
    df_transactions = pd.DataFrame(all_transactions)[expected_columns]
   
    df_transactions["transactionCreatedDate"] = pd.to_datetime(df_transactions["transactionCreatedDate"].str[:10], format="%Y-%m-%d").dt.strftime("%m/%d/%y")
    df_transactions["transactionEffectiveDate"] = pd.to_datetime(df_transactions["transactionEffectiveDate"].str[:10], format="%Y-%m-%d").dt.strftime("%m/%d/%y")
 
    #Adding a timestamp columns
    today = pd.Timestamp.now(tz='UTC').tz_convert('US/Pacific')
    current_datetime = today.strftime("%Y-%m-%dT%H:%M:%S")
    df_transactions['timestamp'] = current_datetime
 
    return df_transactions
 
@task
def email_csv(df_transactions):
 
    df_transactions.to_csv(r'C:\data-analysis\TestCSVFiles\Transaction_test.csv', index=False)
 
    # Save the CSV file
    csv_path = r'C:\data-analysis\TestCSVFiles\Transaction_test.csv'
    df_transactions.to_csv(csv_path, index=False)
 
    # Read the CSV file and encode it as base64 for attachment
    with open(csv_path, 'rb') as csv_file:
        csv_data = csv_file.read()
        encoded_csv = base64.b64encode(csv_data).decode()
 
    # Set up Postmark client and send email with attachment
    postmark_server_token_prefect = postmark_server_token.get()
    postmark = PostmarkClient(server_token=postmark_server_token_prefect)
 
    postmark.emails.send(
        From='legacy-info@rook.capital',
        To=['daniell@legacyg.com'],
        Subject=f"Updated Transaction Report",
        HtmlBody="Please find the updated transaction report attached.",
        Attachments=[
            {
                "Name": "Transaction_test.csv",
                "Content": encoded_csv,
                "ContentType": "text/csv"
            }
        ]
    )
 
@task
def get_dynamic_filename():
 
 
  ## Generate a dynamic filename based on the current date. ##
  ## A string representing the filename with a date-based naming convention. ##
  today = pd.Timestamp.now()
  today_date = today.strftime("%m%d%y_%H%M%S")  # MMDDYYHMS format
  today_date_short = today.strftime("%m%y")  # MMYY format
 
  ## Create a dynamic naming convention using the date ##
  dynamic_filename = f"transaction_report_{today_date}.csv"
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
 
 
 
@flow (log_prints= True)
def TRANSACTION_REPORT() -> None:
 
    api_token = authenticate()
    id_values = GetReportData(api_token)
    df_transactions = GetTransactions (id_values, api_token)
    email_csv(df_transactions)
    current_month = pd.Timestamp.now().strftime("%m-%y")
    current_month_folder = f"{current_month}"
    folder = "land-gorilla-transaction-report"
    destination_blob_name = f"{folder}/{current_month_folder}/{get_dynamic_filename()}"
    gcs_bucket_block_name = "gcs-test-block"
    upload_csv_to_bucket_with_block(gcs_bucket_block_name, df_transactions, destination_blob_name)
