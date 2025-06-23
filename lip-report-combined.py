import snowflake.connector
import base64
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from datetime import datetime
from prefect import task, flow
from postmarker.core import PostmarkClient
from prefect.blocks.system import Secret
from configuration.config import Config
from io import BytesIO
from io import StringIO
from prefect_gcp.cloud_storage import GcsBucket
gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-lip-block")
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
postmark_server_token = Secret.load("postmark-token")
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from prefect_gcp import GcpCredentials
gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-lip-funded-block")
gcp_credentials_block = GcpCredentials.load("prefect-creds")  # Load GCP credentials
gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-test-block")  # Load GCS bucket block
from prefect_snowflake import SnowflakeCredentials
snowflake_credentials_block = SnowflakeCredentials.load("snowflake-credentials-block")

@task
def connection():
    creds = SnowflakeCredentials.load("snowflake-credentials-block")

    # Extract PEM and passphrase from the credentials block
    private_key_pem = creds.private_key.get_secret_value()
    private_key_passphrase = creds.private_key_passphrase.get_secret_value().encode()

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
        user=creds.user,
        private_key=private_key_der,
        account=creds.account,
        warehouse="LEGACY_SMALL",
        database="LEGACY_RDA",
        schema="REPORTING"
    )

    conn.cursor().execute("USE WAREHOUSE LEGACY_SMALL")
    conn.cursor().execute("USE DATABASE LEGACY_RDA")
    conn.cursor().execute("USE SCHEMA REPORTING")

    # Getting the lip_point_latest view
    cur1 = conn.cursor()
    df1 = cur1.execute('select * from LEGACY_RDA.REPORTING.LIP_POINT')
    lip_point_latest = pd.DataFrame.from_records(
        iter(cur1), columns=[col[0] for col in cur1.description]
    )

    # Getting the lip_encompass_latest view
    cur2 = conn.cursor()
    df2 = cur2.execute('select * from LEGACY_RDA.REPORTING.LIP_ENCOMPASS')
    lip_encompass_latest = pd.DataFrame.from_records(
        iter(cur2), columns=[col[0] for col in cur2.description]
    )

    return lip_encompass_latest, lip_point_latest
     
@task
def merge_reports(lip_encompass_latest, lip_point_latest):

    columns_to_keep = [
        "LOAN_NUMBER", "TBD", "NOTES","BORROWER_FIRST_NAME", "BORROWER_LAST_NAME", "STREET", "STATE", 
        "CLOSING_DATE", "LOCK_EXPIRATION", "LOAN_AMOUNT", "NOTE_RATE", "POINTS", 
        "WIRE_AMOUNT", "LO_BROKER", "BP_DOC_REVIEW", "APPROVED", "CLEAR_TO_CLOSE", 
        "APPRAISAL_ORDERED", "APPRAISAL_DUE", "APPRAISAL_EXPIRATION", "TITLE_ORDERED", 
        "TITLE_RECEIVED", "DOCS_OUT", "SIGNING_APPOINTMENT", "FUND","LOAN_TYPE", "CLOSING_LIKELIHOOD","OCCUPANCY",
        "LIEN_POSITION", "LOAN_TERM"
    ]

    lip_point_latest = lip_point_latest[columns_to_keep]


    lip_point_latest.set_index(["LOAN_NUMBER","TBD","NOTES","BORROWER_FIRST_NAME","BORROWER_LAST_NAME","STREET","STATE","CLOSING_DATE","LOCK_EXPIRATION","LOAN_AMOUNT","NOTE_RATE","POINTS","WIRE_AMOUNT","LO_BROKER",\
    "LOAN_TYPE","BP_DOC_REVIEW","APPROVED","CLEAR_TO_CLOSE","APPRAISAL_ORDERED","APPRAISAL_DUE","APPRAISAL_EXPIRATION","TITLE_ORDERED","TITLE_RECEIVED","DOCS_OUT","SIGNING_APPOINTMENT","FUND","OCCUPANCY","LIEN_POSITION","LOAN_TERM"])
  

    lip_encompass_latest.rename(columns={'TOTAL_LOAN_AMOUNT': 'LOAN_AMOUNT', 'TITLE_ORDERED_DATE': 'TITLE_ORDERED', 'EST_CLOSING_DATE':'CLOSING_DATE', 'SUBJECT_PROPERTY_ADDRESS':'STREET', 'LGC_FUND_SOURCE':'FUND','LOAN_PROGRAM':'LOAN_TYPE',\
    'LOAN_OFFICER':'LO_BROKER', 'TITLE_ORDERED_DATE':'TITLE_ORDERED', 'TITLE_DOC_RECEIVED_DATE':'TITLE_RECEIVED','LOCK_EXPIRATION_DATE':'LOCK_EXPIRATION','ORIGINATION_CHARGE':'POINTS','APPRAISAL_ORDERED_DATE':'APPRAISAL_ORDERED','APPRAISAL_ESTIMATED_DUE_DATE':'APPRAISAL_DUE',\
    'SUBJECT_PROPERTY_STATE':'STATE', 'UNDERWRITING_APPROVAL_DATE':'APPROVED','UNDERWRITING_CLEAR_TO_CLOSE_DATE':'CLEAR_TO_CLOSE','TOTAL_WIRE_TRANSFER':'WIRE_AMOUNT','UNDERWRITING_APPRAISAL_EXPIRED_DATE':'APPRAISAL_EXPIRATION'}, inplace=True)

    columns_to_keep = ["LOAN_NUMBER","BORROWER_FIRST_NAME","BORROWER_LAST_NAME", "NOTE_RATE", "LOAN_AMOUNT","CLOSING_DATE","STREET","LIEN_POSITION", "FUND", "LOAN_TYPE","LO_BROKER", "TITLE_ORDERED", "TITLE_RECEIVED","LOCK_EXPIRATION","POINTS","APPRAISAL_ORDERED","APPRAISAL_DUE",\
    "STATE", "APPROVED","CLEAR_TO_CLOSE","WIRE_AMOUNT","APPRAISAL_EXPIRATION","CLOSING_LIKELIHOOD","OCCUPANCY","LOAN_TERM"]

    lip_encompass_latest = lip_encompass_latest[columns_to_keep]

    lip_encompass_latest.set_index(["LOAN_NUMBER","BORROWER_FIRST_NAME","BORROWER_LAST_NAME", "NOTE_RATE", "LOAN_AMOUNT","CLOSING_DATE","STREET","LIEN_POSITION","FUND", "LOAN_TYPE","LO_BROKER","TITLE_ORDERED","TITLE_RECEIVED","LOCK_EXPIRATION","POINTS","APPRAISAL_ORDERED","APPRAISAL_DUE",\
    "STATE", "APPROVED","CLEAR_TO_CLOSE","WIRE_AMOUNT", "APPRAISAL_EXPIRATION","OCCUPANCY","LOAN_TERM"])

    lip_encompass_latest

    concatenated_table = pd.concat([lip_point_latest, lip_encompass_latest], axis=0, ignore_index=True)

    #normalizing the values

    #first let's strip away quotes
    ##concatenated_table = concatenated_table.replace({"'": "", '"': ""}, regex=True)##

    #then let's convert to datetime

    columns_to_convert = ['CLOSING_DATE', 'BP_DOC_REVIEW','APPRAISAL_ORDERED','APPRAISAL_DUE','APPRAISAL_EXPIRATION','TITLE_ORDERED','TITLE_RECEIVED','DOCS_OUT','SIGNING_APPOINTMENT']
    concatenated_table[columns_to_convert] = concatenated_table[columns_to_convert].apply(pd.to_datetime, errors='coerce')

    #replacing various string values for consistancy between the two combined dataframes

    concatenated_table['LIEN_POSITION'] = concatenated_table['LIEN_POSITION'].replace('First Lien', 'First')

    #turning columns to upper case
    concatenated_table['LOAN_NUMBER'] = concatenated_table['LOAN_NUMBER'].str.upper()


    column_order = ['LOAN_NUMBER', 'TBD', 'NOTES', 'BORROWER_FIRST_NAME',
        'BORROWER_LAST_NAME', 'STREET', 'STATE', 'CLOSING_DATE','CLOSING_LIKELIHOOD',
        'LOCK_EXPIRATION', 'LOAN_AMOUNT', 'NOTE_RATE', 'POINTS', 'WIRE_AMOUNT',
        'LO_BROKER', 'BP_DOC_REVIEW', 'APPROVED', 'CLEAR_TO_CLOSE',
        'APPRAISAL_ORDERED', 'APPRAISAL_DUE', 'APPRAISAL_EXPIRATION',
        'TITLE_ORDERED', 'TITLE_RECEIVED', 'DOCS_OUT', 'SIGNING_APPOINTMENT',
        'FUND', 'LOAN_TYPE', 'OCCUPANCY',
        'LIEN_POSITION','LOAN_TERM']

    concatenated_table = concatenated_table[column_order]

    # Ensure CLOSING_DATE is in datetime format
    concatenated_table['CLOSING_DATE'] = pd.to_datetime(concatenated_table['CLOSING_DATE'])

    # Sort by CLOSING_DATE (by month and oldest first) and then by FUND
    concatenated_table = concatenated_table.sort_values(
        by=['CLOSING_DATE', 'FUND'],  # Primary sort by date, secondary by fund
        ascending=[True, True]        # Both in ascending order
    )

    # Reorder columns
    column_order = ['LOAN_NUMBER', 'TBD', 'NOTES', 'BORROWER_FIRST_NAME',
                    'BORROWER_LAST_NAME', 'STREET', 'STATE', 'CLOSING_DATE', 'CLOSING_LIKELIHOOD',
                    'LOCK_EXPIRATION', 'LOAN_AMOUNT', 'NOTE_RATE', 'POINTS', 'WIRE_AMOUNT',
                    'LO_BROKER', 'BP_DOC_REVIEW', 'APPROVED', 'CLEAR_TO_CLOSE',
                    'APPRAISAL_ORDERED', 'APPRAISAL_DUE', 'APPRAISAL_EXPIRATION',
                    'TITLE_ORDERED', 'TITLE_RECEIVED', 'DOCS_OUT', 'SIGNING_APPOINTMENT',
                    'FUND', 'LOAN_TYPE', 'OCCUPANCY', 'LIEN_POSITION','LOAN_TERM']

    concatenated_table = concatenated_table[column_order]


    # Adding A Current Date Column For Snowflake Differentiation

    today = pd.Timestamp.now(tz='UTC').tz_convert('US/Pacific')
    current_datetime = today.strftime("%Y-%m-%dT%H:%M:%S")
    concatenated_table['timestamp'] = current_datetime


    concatenated_table = concatenated_table.replace({'null': None})

    numeric_columns = ['LOAN_AMOUNT', 'POINTS', 'WIRE_AMOUNT', 'NOTE_RATE']
    concatenated_table[numeric_columns] = concatenated_table[numeric_columns].apply(pd.to_numeric, errors='coerce')


    return concatenated_table


@task
def email_csv(concatenated_table):

    concatenated_table.to_csv(r'C:\data-analysis\TestCSVFiles\LIPtest.csv', index=False)

    # Save the CSV file
    csv_path = r'C:\data-analysis\TestCSVFiles\LIPtest.csv'
    concatenated_table.to_csv(csv_path, index=False)

    # Read the CSV file and encode it as base64 for attachment
    with open(csv_path, 'rb') as csv_file:
        csv_data = csv_file.read()
        encoded_csv = base64.b64encode(csv_data).decode()

    # Set up Postmark client and send email with attachment
    postmark_server_token_prefect = postmark_server_token.get()
    postmark = PostmarkClient(server_token=postmark_server_token_prefect)
    current_date = datetime.today().strftime('%Y-%m-%d')

    postmark.emails.send(
        ##list of emails 'nickw@legacyg.com','KellieR@legacyg.com','coltonf@legacyg.com'##
        From='legacy-info@rook.capital',
        To=['dustinh@legacyg.com','nickw@legacyg.com','coltonf@legacyg.com','PatrickK@legacyg.com'],
        Subject=f"Updated Lip Report - {current_date}",
        HtmlBody=f"Please find the updated LIP report attached as of {current_date}.",
        Attachments=[
            {
                "Name": f"LIP - {current_date}.csv",
                "Content": encoded_csv,
                "ContentType": "text/csv"
            }
        ]
    )

@task
def get_dynamic_filename():
  
  
  ## Generate a dynamic filename based on the current date. ##
  ## A string representing the filename with a date-based naming convention. ##
  today = pd.Timestamp.now().normalize()
  today_date = today.strftime("%m%d%y")  # MMDDYY format
  today_date_short = today.strftime("%m%y")  # MMYY format
  
  ## Create a dynamic naming convention using the date ##
  dynamic_filename = f"lip_combined_{today_date}.csv"
  return dynamic_filename

@task
def upload_csv_to_bucket_with_block(gcs_bucket_block_name: str, df: pd.DataFrame, destination_blob_name: str):
  try:
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    gcs_bucket = GcsBucket.load("gcs-lip-block")

    gcs_bucket.upload_from_file_object(csv_buffer, destination_blob_name)
    print(f"DataFrame uploaded to GCS bucket as {destination_blob_name} using block {gcs_bucket_block_name}.")
  except Exception as e:
    print(f"Error occurred while uploading file to GCS: {e}")



@flow (log_prints= True)
def LIP_REPORT() -> None:
    lip_encompass_latest, lip_point_latest = connection()
    concatenated_table = merge_reports(lip_encompass_latest, lip_point_latest)
    email_csv(concatenated_table)
    current_month = pd.Timestamp.now().strftime("%m-%y")
    current_month_folder = f"{current_month}"
    destination_blob_name = f"{current_month_folder}/{get_dynamic_filename()}"
    gcs_bucket_block_name = "gcs-test-block"
    upload_csv_to_bucket_with_block(gcs_bucket_block_name, concatenated_table, destination_blob_name)
    


