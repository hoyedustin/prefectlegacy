import requests
import json
from datetime import datetime
from prefect import task, flow
import os
import pandas as pd
from postmarker.core import PostmarkClient
from prefect.blocks.system import Secret
github_access_token = Secret.load("github-access-token")
dustin_sf_un = Secret.load("dustin-salesforce-un")
dustin_sf_pw = Secret.load("dustin-salesforce-pw")
dustin_sf_security_token = Secret.load("dustin-salesforce-security-token")
lg_pw = Secret.load("lg-api-pw")
lg_un = Secret.load("lg-api-un")
postmark_server_token = Secret.load("postmark-token")
from simple_salesforce import Salesforce
sf = Salesforce(username=dustin_sf_un.get(), password=dustin_sf_pw.get(), security_token=dustin_sf_security_token.get(),instance= 'https://legacycapitalgroup.my.salesforce.com/lightning/o/Case/list?filterName=All_Servicing_Cases/')
#testing under this line for hashlib
import hashlib


# Task 1: Authentication
@task
def authenticate():
    GetTokenURL = "https://clmapi.landgorilla.com/api/token"
    USER = lg_un.get()
    PW = lg_pw.get()
    body = {'api_name': 'clm'}
    headers = {
        'USER': USER,
        'PASSWORD': PW
    }
    response = requests.get(GetTokenURL, headers=headers, json=body)
    json_data = response.json()
    return json_data.get('token')
 

# Task 2: Get Template ID

@task
def get_template_id(api_token):
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

    return template_id
    
# Task 3: Get Specific Loan

@task
def get_loan(template_id, loan_id, api_token):
    GetLoanURL = f"https://clmapi.landgorilla.com/api/clm/pipelineReportTemplates/{template_id}/loans/{loan_id}"
    headers = {
        'Authorization': f'Bearer {api_token}'
    }
    GetLoanResponse = requests.get(GetLoanURL, headers=headers, json={})
    loan_data = GetLoanResponse.json().get('data', {}).get('fields', {})
    print(f"token: {api_token}, loan data: {loan_data}, load id: {loan_id}")
    return loan_data

# Task 4: Get specific draw
# Coming soon
    

# Task 5: Send Email Notification


@task
def create_case_sf (loan_data, updated_status, draw_id, api_token):
    
    draw_list_url = f"https://clmapi.landgorilla.com/api/clm/draw/{draw_id}"
    headers = {
        'Authorization': f'Bearer {api_token}'
    }
    body = {'api_name': 'clm'}
    draw_id_response = requests.get(draw_list_url, headers=headers, json=body)
    draw_data = draw_id_response.json()
    draw_type = draw_data.get('type')
    risk_labels = loan_data.get('riskLabel', [])
    loan_number = loan_data.get('loanNumber', [])
    borrower = loan_data.get('borrower', [])
    borrower_first_name = borrower.get('firstName', '')
    borrower_last_name = borrower.get('lastName', '')
    property_address = loan_data.get('propertyAddress', [])
    should_create_sf_case_draw_submit = updated_status.strip().lower() == 'draw submit'
    print(risk_labels)
    print(loan_number)
    print(updated_status)
    print(draw_id)
    print(draw_type)

    #Duplicate Filtering
    query = "SELECT Loan_Number__c, Draw_ID__c FROM Case WHERE Status = 'New'"
    open_cases = sf.query(query)

    # Convert the records to a DataFrame
    df = pd.DataFrame(open_cases['records'])

    # Drop the extra attributes column (if present)
    if 'attributes' in df.columns:
        df = df.drop(columns=['attributes'])

    # Function to create a hash from loan number and draw ID
    def create_hash(loan_number, draw_id):
        combined_string = f"{loan_number}-{draw_id}"
        return hashlib.sha256(combined_string.encode()).hexdigest()

    # Create hash for the new data
    new_case_hash = create_hash(loan_number, draw_id)

    # Function to create a hash from loan number and draw ID for each row in df
    def create_df_hash(row):
        combined_string_df = f"{row['Loan_Number__c']}-{row['Draw_ID__c']}"
        return hashlib.sha256(combined_string_df.encode()).hexdigest()

    df['Hash'] = df.apply(create_df_hash, axis=1)


    if new_case_hash in df['Hash'].values:
        print("Duplicate case detected. No new case will be created.")
        return
        
    #create case for draw submits
    if should_create_sf_case_draw_submit and draw_type != None:
        case_data = {
            'Subject': f"Pending Draw Created in LG  Loan Number: {loan_number}",
            'Description': f"{loan_number}",
            'RecordTypeId': '0128c000001eZ6PAAU',
            'OwnerId': '00G8c0000068j8mEAA',
            'Reason' : 'Draw',
            'Loan_Number__c': f"{loan_number}",
            'Address__Street__s': f"{property_address}",
            'Draw_ID__c': f"{draw_id}",
            'Borrower__c': f"{borrower_first_name} {borrower_last_name}"
            # Add other required fields here
        }
        try:
            case = sf.Case.create(case_data)
            print('Case created successfully:', case)
        
        except Exception as e:
            print('Error creating case:', e)
           
    else:
            print(f"status is not 'Draw Submit'. No case created. loan#:{loan_number}, risk label: {risk_labels}")
        
    #create case for draw create and accounting in risk labels
    #if 'Accounting' in risk_labels and should_create_sf_case_draw_submit and draw_type != None:
    #    case_data = {
    #        'Subject': f"Pending Draw Created in LG  Loan Number: {loan_number}",
    #        'Description': f"{loan_number}",
    #        'RecordTypeId': '0128c000001eZ6PAAU',
    #        'OwnerId': '00G8c0000068j8mEAA',
    #        'Reason' : 'Draw',
    #        'Loan_Number__c': f"{loan_number}",
    #        'Address__Street__s': f"{property_address}",
    #        'Draw_ID__c': f"{draw_id}",
    #        'Borrower__c': f"{borrower_first_name} {borrower_last_name}"
    #        # Add other required fields here
    #    }
    #    try:
    #        case = sf.Case.create(case_data)
    #        print('Case created successfully:', case)
        
    #    except Exception as e:
    #        print('Error creating case:', e)
           
    #else:
    #        print(f"Risk label does not contain 'Accounting' or updated status is not 'Draw Create'. No email sent. loan#:{loan_number}, risk label: {risk_labels}")



    

@task
def send_email(loan_data, updated_status):
    risk_labels = loan_data.get('riskLabel', [])
    loan_number = loan_data.get('loanNumber', [])
    should_send_email = updated_status.strip().lower() == 'draw submit'
    print(risk_labels)
    print(loan_number)
    print(updated_status)



    if 'Accounting' in risk_labels and should_send_email:
            postmark_server_token_prefect = postmark_server_token.get()
            postmark = PostmarkClient (server_token= postmark_server_token_prefect)
            postmark.emails.send(
                 From= 'legacy-info@rook.capital',
                 To='DL_Acct_Staff@legacyg.com',
                 Subject= f"Draw Change for {loan_number}",
                 HtmlBody= f"Loan Number: {loan_number}\nRisk Label(s): {risk_labels}\nUpdated Status: {updated_status}"
                 )
    else:
            print(f"Risk label does not contain 'Accounting' or updated status is not 'Draw Submit'. No email sent. loan#:{loan_number}, risk label: {risk_labels}")


@flow (log_prints= True)
def Accounting_webhook(loan_id: str, updated_status: str, draw_id:str) -> None:
    api_token = authenticate()
    template_id = get_template_id(api_token)
    loan_data = get_loan(template_id, loan_id, api_token)
    create_case_sf (loan_data, updated_status, draw_id, api_token)
    send_email(loan_data, updated_status)
