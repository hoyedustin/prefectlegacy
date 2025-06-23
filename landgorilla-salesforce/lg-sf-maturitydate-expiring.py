import requests
import json
from datetime import datetime, timezone
import pytz
import os
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
github_access_token = Secret.load("github-access-token")
dustin_sf_un = Secret.load("dustin-salesforce-un")
dustin_sf_pw = Secret.load("dustin-salesforce-pw")
dustin_sf_security_token = Secret.load("dustin-salesforce-security-token")
lg_pw = Secret.load("lg-api-pw")
lg_un = Secret.load("lg-api-un")
postmark_server_token = Secret.load("postmark-token")
#testing under this line for creation in SF
from simple_salesforce import Salesforce
sf = Salesforce(username=dustin_sf_un.get(), password=dustin_sf_pw.get(), security_token=dustin_sf_security_token.get(),instance= 'https://legacycapitalgroup.my.salesforce.com/lightning/o/Case/list?filterName=All_Servicing_Cases/')
local_tz = pytz.timezone("America/Los_Angeles")

 
# Task 1: Authentication
@task
def authenticate():
  GetTokenURL = "https://clmapi.landgorilla.com/api/token"
  USER = "dustinh@legacyg.com"
  PW = "Index@2043!"
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
    report_name = 'AccountingAllActiveLoans'
   
    latest_report = max((data for data in ReportData if data['name'] == report_name), key=lambda x: int(x['id']), default=None)
    latest_id_value = latest_report.get('id')
 
    return latest_id_value
 
@task
def pull_accounting_all_active_loans_report(latest_id_value, api_token):
 
    logger = get_run_logger()  
 
    ActualReportURL = f"https://clmapi.landgorilla.com/api/clm/pipelineReport/{latest_id_value}"
    headers = {
        'Authorization': f'Bearer {api_token}'
        }
   
    ActualReportResponse = requests.get(ActualReportURL, headers=headers)
    ReportData = ActualReportResponse.json()
    data_list = ReportData['data']['Current Report']['data']
    df = pd.DataFrame(data_list)
   
    df['Current Loan Due Date'] = pd.to_datetime(df['Current Loan Due Date'])
    df['Loan Funded Date'] = pd.to_datetime(df['Loan Funded Date'])
 
   
 
    today = pd.Timestamp.now().normalize()  # normalize to remove time part
 
    #utc_datetime_today = today.astimezone(pytz.utc)
 
    logger.info(today)
 
    # Calculate the distance in days between today's date and the loan due date
    df['Days Until Loan Due'] = (df['Current Loan Due Date'] - today).dt.days
 
    need_to_extend_df = df[df['Days Until Loan Due'] == 30]
   
    return need_to_extend_df
 
 
@task
def create_sf_case(need_to_extend_df):
 
    logger = get_run_logger()
 
    if need_to_extend_df.empty:
        logger.info("No loans are due in 30 days. Exiting the script.")
        return
   
 
    for index, row in need_to_extend_df.iterrows():
        if row['Days Until Loan Due'] == 30:
            case_data = {
                'Subject': f"Maturity date expiring in 30 days for loan number: {row['Loan Number']}",
                'Description': row['Loan Number'],
                'RecordTypeId': '0128c000001eZ6ZAAU',
                'OwnerId': '00G8c0000068j8mEAA',
                'Reason' : 'Maturity Date: Expires in 30 days',
                'Loan_Number__c': row['Loan Number'],
                'Address__Street__s': row['Property Address'],
                'Borrower__c': row['Borrower First Name'] +" "+ row['Borrower Last Name']
        }
       
        sf.Case.create(case_data)
 
 
@flow
def lg_to_sf_maturity_date_case_create() -> None:
    api_token = authenticate()
    latest_id_value = get_most_recent_id_value(api_token)
    need_to_extend_df = pull_accounting_all_active_loans_report(latest_id_value, api_token)
    create_sf_case(need_to_extend_df)
