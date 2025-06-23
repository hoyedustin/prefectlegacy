import pandas as pd
import requests
from prefect import flow, task
from prefect import get_run_logger
import snowflake.connector
from datetime import datetime
from prefect.blocks.system import Secret
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from prefect_snowflake import SnowflakeCredentials
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud import DbtCloudCredentials

snowflake_creds = SnowflakeCredentials.load("snowflake-credentials-block")

job_id = "70471823452836"
dbt_cloud_credentials = DbtCloudCredentials.load("prefect-dbt")

@task
def trigger_dbt_cloud_job(dbt_cloud_credentials, job_id):

    account_id = dbt_cloud_credentials.account_id
    api_token = dbt_cloud_credentials.api_key.get_secret_value()

    url = f"https://xx165.us1.dbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "cause": "Triggered via Prefect API call"
    }

    response = requests.post(url, headers=headers, json=payload)

    print("Full Response:")
    print("Status code:", response.status_code)
    try:
        response_json = response.json()
        print("Response JSON:", response_json)
    except ValueError:
        print("Response text:", response.text)

    if response.status_code == 200:
        run_id = response.json()["data"]["id"]
        print(f"Run triggered successfully! Run ID: {run_id}")
    elif response.status_code == 401:
        print("Unauthorized. The API token may be invalid.")
    else:
        print(f"Failed to trigger run. Status code: {response.status_code}")
        print(f"Response: {response.text}")

@task
def snowflake_connect(snowflake_creds):

    # Extract PEM and passphrase from the credentials block
    private_key_pem = snowflake_creds.private_key.get_secret_value()
    private_key_passphrase = snowflake_creds.private_key_passphrase.get_secret_value().encode()

    # Decrypt the private key
    private_key = serialization.load_pem_private_key(
        private_key_pem,
        password=private_key_passphrase,
        backend=default_backend()
    )

    # Convert to DER
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_creds.user,
        private_key=private_key_der,
        account=snowflake_creds.account,
        warehouse="LEGACY_SMALL",
        database="TRANSFORM",
        schema="NETSUITE"
    )


    conn.cursor().execute("USE WAREHOUSE LEGACY_SMALL")
    conn.cursor().execute("USE DATABASE TRANSFORM")
    conn.cursor().execute("USE SCHEMA NETSUITE")
    
    cur = conn.cursor()

    cur.execute('SELECT * FROM FINAL_CORE_VALUES_TRANSFORMATION')
    df = cur.fetch_pandas_all()   # <-- this actually grabs the query results as a DataFrame
    df = df.astype(str)
    new_column_names = [
        "lg_loan_id",
        "lg_loan_number",
        "coreSystemLoanId",
        "coreSystemName",
        "coreSystemLoanAmount",
        "coreSystemLoanBalance",
        "coreSystemLoanBalanceLastUpdated"
    ]

    # Apply the new column names
    df.columns = new_column_names

    logger = get_run_logger()
    logger.info(df)
    
    return df


@task
def get_api_token():
    url = "https://clmapi.landgorilla.com/api/token"
    headers = {'USER': 'dustinh@legacyg.com', 'PASSWORD': 'Index@2043!'}
    response = requests.get(url, headers=headers, json={'api_name': 'clm'})
    response.raise_for_status()
    return response.json().get('token')
 
@task
def process_loan(df, api_token):

    for _, row in df.iterrows():
        loan_id = row['lg_loan_id']

        get_url = f"https://clmapi.landgorilla.com/api/clm/loan/{loan_id}/coreValues"
        headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

        get_response = requests.get(get_url, headers=headers)

        if get_response.status_code == 200 and get_response.json():
            print(f"Values found for Loan ID {loan_id}, proceeding with PATCH...")
            payload = row.to_dict()
            patch_response = requests.patch(get_url, json=payload, headers=headers)
            print(f"PATCH Response Status Code for Loan ID {loan_id}: {patch_response.status_code}")
            print(f"PATCH Response Text for Loan ID {loan_id}: {patch_response.text}")
        else:
            print(f"No values found for Loan ID {loan_id}. Skipping PATCH.")

 


@flow
def CORE_VALUES():
    trigger_dbt_cloud_job(dbt_cloud_credentials, job_id)
    df = snowflake_connect(snowflake_creds)
    api_token = get_api_token()
    process_loan(df, api_token)

 
