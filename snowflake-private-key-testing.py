import snowflake.connector
import pandas as pd
from datetime import datetime
from prefect import task, flow
from prefect.blocks.system import Secret
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
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

    return conn

@task
def confirm_connection(conn):

    # Getting the funded_point_latest view
    cur1 = conn.cursor()
    df1 = cur1.execute('select * from LIP_POINT_FUNDED')
    funded_point_latest = pd.DataFrame.from_records(iter(cur1), columns=[col[0] for col in cur1.description])

    # Getting the funded_encompass_latest view
    cur2 = conn.cursor()
    df2 = cur2.execute('select * from LIP_ENCOMPASS_FUNDED')
    funded_encompass_latest = pd.DataFrame.from_records(iter(cur2), columns=[col[0] for col in cur2.description])

    print(funded_encompass_latest)
    print(funded_point_latest)


@flow (log_prints= True)
def SNOWFLAKE_CONNCECT_TEST() -> None:
    conn = connection()
    confirm_connection(conn)
