from re import sub
import os, sys

parent_dir = os.path.abspath("..")
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from uuid import uuid4

from configuration.config import Config
from load.loader import LoaderBase
from model.snowflake_connector import SnowflakeConnector


class PaychexCompanyLoader(LoaderBase):
    TABLE_NAME = "PAYCHEX_COMPANIES"

    def __init__(self, config: Config):
        super().__init__(config)

    def transform(self, data) -> pd.DataFrame:
        # Transform the data from the Paychex API response into a data frame
        # that conforms to the Snowflake table schema.
        columns = {
            "companyId": "company_id",
            "displayId": "display_id",
            "legalName": "legal_name",
        }

        df = pd.DataFrame(data, columns=columns.keys())
        df.rename(columns=columns, inplace=True)
        df.astype(
            {
                "company_id": str,
                "display_id": str,
                "legal_name": str,
            }
        )

        return df

    def write(self, data) -> None:
        # Persist the transformed data to Snowflake. The data is written to a
        # temporary table, which is then merged with the main table to ensure
        # performant data synchronization.
        batch = sub("-", "", str(uuid4()))[:10].upper()
        df = self.transform(data)

        # Snowflake automatically converts column names (and other identifiers)
        # to uppercase unless they are enclosed in double quotes.
        create_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{self.TABLE_NAME} (
            "company_id" VARCHAR(255) PRIMARY KEY,
            "display_id" VARCHAR(255) NOT NULL,
            "legal_name" VARCHAR(255) NOT NULL
        )
        """

        merge_table = f"""
        MERGE INTO {self.database}.{self.schema}.{self.TABLE_NAME} AS target
            USING {self.database}.{self.schema}.{self.TABLE_NAME}_{batch} AS source
            ON target."company_id" = source."company_id"
            WHEN MATCHED THEN
            UPDATE SET
                target."display_id" = source."display_id",
                target."legal_name" = source."legal_name"
            WHEN NOT MATCHED THEN
            INSERT ("company_id", "display_id", "legal_name")
            VALUES (source."company_id", source."display_id", source."legal_name");
        """

        with SnowflakeConnector(
            self.username, self.account, self.database, self.schema, self.warehouse, self.private_key, self.passphrase
        ).connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"USE WAREHOUSE {self.warehouse}".upper())
                cursor.execute(f"{create_table}")

                write_pandas(
                    conn=connection,
                    df=df,
                    table_name=f"{self.TABLE_NAME}_{batch}",
                    database=self.database,
                    schema=self.schema,
                    auto_create_table=True,
                    table_type="temporary",
                )

                cursor.execute(merge_table)
