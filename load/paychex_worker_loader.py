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


class PaychexWorkerLoader(LoaderBase):
    TABLE_NAME = "PAYCHEX_WORKERS"

    def __init__(self, config: Config):
        super().__init__(config)

    def transform(self, data) -> pd.DataFrame:
        # Transform the data from the Paychex API response into a data frame
        # that conforms to the Snowflake table schema. In order to simplify
        # the transformation to a flat data model, synthetic columns are added
        #  to the provided employee data.
        for row in data:
            name = row.get("name", {})
            row["firstName"] = name.get("givenName", None)
            row["lastName"] = name.get("familyName", None)
            status = row.get("currentStatus", {})
            row["status"] = status.get("statusType", None)

        columns = {
            "workerId": "id",
            "employeeId": "employee_id",
            "firstName": "first_name",
            "lastName": "last_name",
            "status": "status",
        }

        df = pd.DataFrame(data, columns=columns.keys())
        df.rename(columns=columns, inplace=True)
        df.astype(
            {
                "id": str,
                "employee_id": str,
                "first_name": str,
                "last_name": str,
                "status": str,
            }
        )

        return df

    def write(self, data) -> None:
        batch = sub("-", "", str(uuid4()))[:10].upper()
        df = self.transform(data)

        # Snowflake automatically converts column names (and other identifiers)
        # to uppercase unless they are enclosed in double quotes.
        create_table = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{self.schema}.{self.TABLE_NAME} (
            "id" VARCHAR(255) PRIMARY KEY,
            "employee_id" VARCHAR(255) NOT NULL,
            "first_name" VARCHAR(255) NOT NULL,
            "last_name" VARCHAR(255) NOT NULL,
            "status" VARCHAR(255) NOT NULL
        );
        """

        merge_table = f"""
        MERGE INTO {self.database}.{self.schema}.{self.TABLE_NAME} AS target
            USING {self.database}.{self.schema}.{self.TABLE_NAME}_{batch} AS source
            ON target."id" = source."id"
            WHEN MATCHED THEN
            UPDATE SET
                target."employee_id" = source."employee_id",
                target."first_name" = source."first_name",
                target."last_name" = source."last_name",
                target."status" = source."status"
            WHEN NOT MATCHED THEN
            INSERT ("id", "employee_id", "first_name", "last_name", "status")
            VALUES (source."id", source."employee_id", source."first_name", source."last_name", source."status");
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
