import os, sys

parent_dir = os.path.abspath("..")
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from model.snowflake_connector import SnowflakeConnector
from datetime import datetime
from configuration.config import Config


class Loader:
    def __init__(
        self, username: str, account: str, database: str, schema: str, warehouse: str, private_key: str, passphrase: str
    ):
        self.username = username
        self.account = account
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.private_key = private_key
        self.passphrase = passphrase

        self.connection = SnowflakeConnector(
            self.username, self.account, self.database, self.schema, self.warehouse, self.private_key, self.passphrase
        ).connect()

    def write_table(self, dataframe: pd.DataFrame, table: str):
        # check to see if the column upload_dates exists
        if "upload_date" in dataframe:
            # filter where updated data is null or not a number
            dataframe["upload_date"] = df["upload_date"].apply(lambda x: datetime.now() if pd.isna(x) else x)

        else:
            dataframe["upload_date"] = datetime.now()

        # let's freakin' go
        self.connection.cursor().execute(f"USE WAREHOUSE {self.warehouse}".upper())
        # this is where column creation will go for upload date fields
        # Add the upload date column if it doesn't already exist

        table = f"{table}".upper()
        write_pandas(
            conn=self.connection,
            df=dataframe,
            table_name=table,
            database=self.database,
            schema=self.schema,
            auto_create_table=True,
        )

        self.connection.cursor().execute(f"DELETE FROM {self.database}.{self.schema}.{table} WHERE \"loan_number\" =''")


class LoaderBase:
    """
    Simplified version of the Loader class that only requires a Config object.
    """

    def __init__(self, config: Config):
        self.username = str(config.snowflake_username)
        self.account = str(config.snowflake_account)
        self.database = "LEGACY_DEMO_RDA"
        self.schema = "LEGACY_DEMO_SCHEMA"
        self.warehouse = "LEGACY_DEMO"
        self.private_key = config.rsa_private_key
        self.passphrase = config.rsa_passphrase
