import requests
import json
from datetime import datetime
from prefect import task, flow
import os
from postmarker.core import PostmarkClient
from prefect.blocks.system import Secret
github_access_token = Secret.load("github-access-token")
lg_pw = Secret.load("lg-api-pw")
lg_un = Secret.load("lg-api-un")
postmark_server_token = Secret.load("postmark-token")
#testing under this line for creation in SF
#from simple_salesforce import Salesforce
#sf = Salesforce(username='dustinh@legacyg.com', password='Rain@2022!', security_token='EBfI0CSZsgQzkzGC1i2Pgn0AL',instance= 'https://legacycapitalgroup.my.salesforce.com/' )


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
    print(f"token: {api_token}, loan data: {loan_data}")
    return loan_data

# Task 4: Send Email Notification


#@task
#def create_case_sf (case_data, updated_status):
#    risk_labels = loan_data.get('riskLabel', [])
#    loan_number = loan_data.get('loanNumber', [])
#    should_create_sf_case = updated_status.strip().lower() == 'draw create'
#    print(risk_labels)
#    print(loan_number)
#    print(updated_status)

 #   #trying some new stuff in SF
 #   if should_create_sf_case:
 #       case_data = {
 #           'Subject': 'Example Subject',
 #           'Description': 'Example Description',
 #           'Status': 'New',
 #           # Add other required fields here
 #       }
#      try:
#          case = sf.Case.create(case_data)
#            print('Case created successfully:', case)
#       except Exception as e:
#          print('Error creating case:', e)
 #          
 #   else:
 #           print(f"status is not 'Draw Create'. No email sent. loan#:{loan_number}, risk label: {risk_labels}")


    

@task
def send_email(loan_data, updated_status):
    risk_labels = loan_data.get('riskLabel', [])
    loan_number = loan_data.get('loanNumber', [])
    should_send_email = updated_status.strip().lower() == 'draw create'
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
            print(f"Risk label does not contain 'Accounting' or updated status is not 'Draw Create'. No email sent. loan#:{loan_number}, risk label: {risk_labels}")


@flow (log_prints= True)
def Accounting_webhook(loan_id: str, updated_status: str) -> None:
    api_token = authenticate()
    template_id = get_template_id(api_token)
    loan_data = get_loan(template_id, loan_id, api_token)
    #create_case_sf (case_data, updated_status)
    send_email(loan_data, updated_status)
