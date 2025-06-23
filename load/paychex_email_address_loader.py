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


class PaychexEmailAddress:
    def __init__(self, id, worker_id, type, email):
        self.id = id
        self.worker_id = worker_id
        self.type = type
        self.email = email

    def as_dict(self):
        return {
            "id": self.id,
            "worker_id": self.worker_id,
            "type": self.type,
            "email": self.email,
        }


class PaychexEmailAddressLoader(LoaderBase):
    TABLE_NAME = "PAYCHEX_EMAIL_ADDRESSES"

    def __init__(self, config: Config):
        super().__init__(config)

    def transform(self, data: list[PaychexEmailAddress]) -> pd.DataFrame:
        columns = {
            "id": "id",
            "worker_id": "worker_id",
            "type": "type",
            "email": "email",
        }

        df = pd.DataFrame([record.as_dict() for record in data], columns=columns.keys())
        df.rename(columns=columns, inplace=True)
        df.astype(
            {
                "id": str,
                "worker_id": str,
                "type": str,
                "email": str,
            }
        )

        df["email"] = df["email"].str.lower()
        return df

    def write(self, data) -> None:
        batch = sub("-", "", str(uuid4()))[:10].upper()

        # Snowflake automatically converts column names (and other identifiers)
        # to uppercase unless they are enclosed in double quotes.
        create_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{self.TABLE_NAME} (
            "id" VARCHAR(255) PRIMARY KEY,
            "worker_id" VARCHAR(255) NOT NULL,
            "type" VARCHAR(255) NOT NULL,
            "email" VARCHAR(255) NOT NULL
        )
        """

        merge_table = f"""
        MERGE INTO {self.database}.{self.schema}.{self.TABLE_NAME} AS target
            USING {self.database}.{self.schema}.{self.TABLE_NAME}_{batch} AS source
            ON target."id" = source."id"
            WHEN MATCHED THEN
            UPDATE SET
                target."worker_id" = source."worker_id",
                target."type" = source."type",
                target."email" = source."email"
            WHEN NOT MATCHED THEN
            INSERT ("id", "worker_id", "type", "email")
            VALUES (source."id", source."worker_id", source."type", source."email");
        """

        with SnowflakeConnector(
            self.username, self.account, self.database, self.schema, self.warehouse, self.private_key, self.passphrase
        ).connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"USE WAREHOUSE {self.warehouse}".upper())
                cursor.execute(f"{create_table}")

                write_pandas(
                    conn=connection,
                    df=self.transform(data),
                    table_name=f"{self.TABLE_NAME}_{batch}",
                    database=self.database,
                    schema=self.schema,
                    auto_create_table=True,
                    table_type="temporary",
                )

                cursor.execute(merge_table)
