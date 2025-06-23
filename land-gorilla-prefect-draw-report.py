import snowflake.connector
import requests
import base64
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from prefect import task, flow, get_run_logger
from postmarker.core import PostmarkClient
from prefect.blocks.system import Secret
from configuration.config import Config
from io import BytesIO
from io import StringIO
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# Load GCP credentials and GCS bucket block
gcp_credentials_block = GcpCredentials.load("prefect-creds")  # Load GCP credentials
gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-test-block")  # Load GCS bucket block

# Postmark server token (Secret management in Prefect)
postmark_server_token = Secret.load("postmark-token")

@task
def prefect_connect():

 
    token = "removed for security"
    account_id= "removed for security"
    workspace_id="removed for security"
    
    url = f"https://api.prefect.cloud/api/accounts/{account_id}/workspaces/{workspace_id}/ui/flow_runs/history"
    
    token = "pnu_2ouAZpMBYyIBpUTWrQhSd505kZjyPV10dtnu"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    #creating time intellgance for date filtering
    
    today = pd.Timestamp.utcnow()
    yesterday = today - pd.Timedelta(days=1)
    
    # First and final minutes of yesterday
    yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
    
    # Formatting the timestamps
    today_str = today.strftime("%Y-%m-%dT%H:%M:%SZ")
    yesterday_str = yesterday.strftime("%Y-%m-%dT%H:%M:%SZ")
    yesterday_start_str = yesterday_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    yesterday_end_str = yesterday_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    
    
    # JSON payload for the request
    payload = {
        "sort": "ID_DESC",
        "limit": 1000,
        "offset": 0,
        "flows": {
            "operator": "and_",
            "id": {
                "any_": [
                    "8fc13498-aa7a-4af4-adc1-0ee58ee61edc"
                ]
            }
        },
        "flow_runs": {
            "start_time": {
                "before_": yesterday_end_str,
                "after_": yesterday_start_str
            }
        }
    }
    
    
    
    
    # Make a POST request
    response = requests.post(url, headers=headers, json=payload)
    report_data = response.json()
    
    df = pd.DataFrame(report_data)

    
    #dropping failed rows b.c those mean the webhook misfired
    if 'state_type' in df.columns:
        df = df[df['state_type'] != 'FAILED']
    
    id = df['id']
    
    all_draws = []
    
    for id in id.astype(str):
    
        token = "removed for security"
        account_id= "removed for security"
        workspace_id="removed for security"
    
        url = f"https://api.prefect.cloud/api/accounts/{account_id}/workspaces/{workspace_id}/flow_runs/{id}"
    
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
        response = requests.get(url, headers=headers)
        json_data = response.json()
        parameters_data = json_data.get('parameters', {})
        all_draws.append(parameters_data)
        if response.status_code == 200:
            print(response.json())
        else:
            print(f"Failed to fetch data for ID {id}, Status Code: {response.status_code}")
    
    prefect_df = pd.DataFrame(all_draws)
    
    return prefect_df

@task
def prefect_cleaning(prefect_df):

    # 1. Initial row count
    print(f"Initial row count: {len(prefect_df)}")

    # 2. Drop duplicates and reset index
    prefect_df_cleaned = prefect_df.drop_duplicates(subset=['draw_id', 'loan_id', 'updated_status'])
    print(f"Row count after drop_duplicates (no reset): {len(prefect_df_cleaned)}")

    # 3. Check the first few rows for hidden issues
    print(prefect_df_cleaned.head())

    # 4. Reset the index after dropping duplicates
    prefect_df_cleaned = prefect_df_cleaned.reset_index(drop=True)
    print(f"Row count after reset_index: {len(prefect_df_cleaned)}")

    # 5. Check if there are any missing or unexpected values
    print(prefect_df_cleaned.isnull().sum())

    # 6. Check for any remaining duplicate rows
    print(f"Rows with duplicates still present: {prefect_df_cleaned.duplicated().sum()}")

    # 7. Inspect a few rows to check for hidden duplicates or issues
    print(prefect_df_cleaned[prefect_df_cleaned.duplicated()])


    return prefect_df_cleaned

@task
def lg_auth():

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
    api_token = json_data.get('token')

    return api_token

@task
def get_loans(prefect_df_cleaned, api_token):

    logger = get_run_logger()

    if 'loan_id' not in prefect_df_cleaned.columns:
        logger.error("No 'loan_id' column found in input data — likely no valid flow run parameters.")
        raise ValueError("Flow failed: 'loan_id' column is missing. This usually means no valid loan-related flow runs occurred.")

    GetTemplateURL = "https://clmapi.landgorilla.com/api/clm/pipelineReportTemplates"
    headers = {
    'Authorization': f'Bearer {api_token}'
        }
    response = requests.get(GetTemplateURL, headers=headers, json={})
    templatedata = response.json()
    print(templatedata)
    for item in templatedata['data']['items']:
        if item['name'] == 'AccountingAllActiveLoans':
            template_id = item['id']


    loan_details_dict = {}

    for loan_id in prefect_df_cleaned['loan_id'].dropna().unique():
        GetLoanURL = f"https://clmapi.landgorilla.com/api/clm/pipelineReportTemplates/{template_id}/loans/{loan_id}"
        
        loan_response = requests.get(GetLoanURL, headers=headers, json={})
        
        if loan_response.status_code == 200:
            loan_details_dict[loan_id] = loan_response.json().get('data', {}).get('fields', {})
            

    loan_df = pd.DataFrame.from_dict(loan_details_dict, orient='index').reset_index()
    loan_df.rename(columns={'index': 'loan_id'}, inplace=True)

    merged_df = prefect_df_cleaned.merge(loan_df, on='loan_id', how='left')

    merged_loan_df = merged_df[['draw_id', 'loan_id', 'updated_status', 'loanNumber']]
  
    #Adding a timestamp columns
    today = pd.Timestamp.now(tz='UTC').tz_convert('US/Pacific')
    current_datetime = today.strftime("%Y-%m-%dT%H:%M:%S")
    merged_loan_df['timestamp'] = current_datetime

    return merged_loan_df

@task
def get_draws(api_token, merged_loan_df):

    all_draws = []  # List to store all draw responses

    for draw_id in merged_loan_df['draw_id'].unique():
        draw_url = f"https://clmapi.landgorilla.com/api/clm/draw/{draw_id}"
        headers = {'Authorization': f'Bearer {api_token}'}
        body = {'api_name': 'clm'}
        
        response = requests.get(draw_url, headers=headers, json=body)
        if response.status_code == 200:
            all_draws.append(response.json())  # Append each response to the list

    # Convert to a dictionary that matches the expected format
    draw_data = {"draws": all_draws} 


    def flatten_draw_data(draw_data):
        rows = []

        draw_list = draw_data.get("draws", [])  

        for draw in draw_list:


            draw_info = {
                "draw_id": draw.get("id"),
                "requestedBy": draw.get("requestedBy"),
                "name": draw.get("name"),
                "type": draw.get("type"),
                "containerNumber": draw.get("containerNumber"),
                "status": draw.get("status"),
                "createdDate": draw.get("createdDate"),
                "submittedDate": draw.get("submittedDate"),
                "approvedDate": draw.get("approvedDate"),
                "effectiveDate": draw.get("effectiveDate"),
                "folderId": draw.get("folderId"),
            }

            line_items = draw.get("lineItems", {})
            if isinstance(line_items, str):
                line_items = json.loads(line_items)

            payee_dict = line_items.get("payee", {})

            for payee_name, payee_data in payee_dict.items():
                items_lists = payee_data.get("items", [])

                for item_list in items_lists:
                    for item in item_list:
                        row = draw_info.copy()
                        row["lineItems.number"] = item.get("number")
                        row["payee.name"] = payee_name                    
                        row["lineItems.description"] = item.get("description")
                        row["lineItems.subDescription"] = item.get("subDescription")
                        row["lineItems.amount"] = item.get("requested")
                        row["lineItems.retainage"] = item.get("retainage")
                        row["lineItems.totalLessRetainage"] = item.get("totalLessRetainage")

                        rows.append(row)

        df = pd.DataFrame(rows)
        return df


    df_draws = flatten_draw_data(draw_data)

    return df_draws

@task
def loan_draw_merge(merged_loan_df, df_draws):

    merged_loan_df["draw_id"] = merged_loan_df["draw_id"].astype(str)

    # Merge on draw_id
    merged_df = merged_loan_df.merge(df_draws, left_on="draw_id", right_on="draw_id", how="left")

    return merged_df

@task
def email_csv(merged_df):

    merged_df.to_csv(r'C:\data-analysis\TestCSVFiles\LIPtest.csv', index=False)

    # Save the CSV file
    csv_path = r'C:\data-analysis\TestCSVFiles\LIPtest.csv'
    merged_df.to_csv(csv_path, index=False)

    # Read the CSV file and encode it as base64 for attachment
    with open(csv_path, 'rb') as csv_file:
        csv_data = csv_file.read()
        encoded_csv = base64.b64encode(csv_data).decode()

    # Set up Postmark client and send email with attachment
    postmark_server_token_prefect = postmark_server_token.get()
    postmark = PostmarkClient(server_token=postmark_server_token_prefect)

    postmark.emails.send(
        From='legacy-info@rook.capital',
        To='DanielL@legacyg.com',
        Subject=f"Updated Draw Report",
        HtmlBody="Please find the updated draw report attached.",
        Attachments=[
            {
                "Name": "DRAW_REPORT_TEST.csv",
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
  dynamic_filename = f"draw_report_{today_date}.csv"
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
def DRAW_REPORT() -> None:
    prefect_df = prefect_connect()
    prefect_df_cleaned = prefect_cleaning(prefect_df)
    api_token = lg_auth()
    merged_loan_df = get_loans(prefect_df_cleaned, api_token)
    df_draws = get_draws(api_token, merged_loan_df)
    merged_df = loan_draw_merge(merged_loan_df, df_draws)
    email_csv(merged_df)
    current_month = pd.Timestamp.now().strftime("%m-%y")
    current_month_folder = f"{current_month}"
    folder = "land-gorilla-draw-report"
    destination_blob_name = f"{folder}/{current_month_folder}/{get_dynamic_filename()}"
    gcs_bucket_block_name = "gcs-test-block"
    upload_csv_to_bucket_with_block(gcs_bucket_block_name, merged_df, destination_blob_name)









