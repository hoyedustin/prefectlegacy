import os, sys
parent_dir = os.path.abspath('..')
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from decimal import *
from transform.transformer import Transformer
import pandas as pd
import numpy as np

class LandGorillaCashStatusCombinedTransformer(Transformer):
    def __init__(self, df:pd.DataFrame):
        super().__init__(df)

    def to_decimal(self, x):
        try:
            if x and x == x and len(x) > 0:
                return round(Decimal(x), 4)
        except (InvalidOperation, ValueError):
            return np.nan
        return np.nan
    
    def transform_data(self)-> None:
        #dropping rows that contain only null values and nothing else in order to get rid of the totals at the bottom
        self.df = self.df.dropna (subset = ['Loan Number'], how = 'all')

        #renaming column
        self.df.rename(columns={"Builder's Risk Insurance Expiration": "Builders Risk Insurance Expiration"}, inplace=True)

        #replacing all empty values with NaN values
        #self.df.replace('', np.nan, inplace=True)

        columns_to_clean = [
            'Collateral Holdback', 'Appraised Value', 'As Permitted Property Value', 'Current Loan Amount',
            'Total Principal Loan Paydown', 'Total Interest Reserve Amount Disbursed', 'Interest Reserve Balance',
            'Loan amount Disbursed excluding Interest Reserve', 'Balance to finish Including Retainage', 'Loan Amount Disbursed'
        ]

        string_columns = [
            'Loan Number', 'Borrower Last Name', 'Property Address', 'Loan Program', 'Loan Term in Months',
            'Loan Originator', 'Total Units', 'County', 'Property Address', 'City', 'Investor', 'Outside Equity',
            'State', 'Zip', 'Exception', 'Additional Collateral Property', 'Funding Account'
        ]

        datetime_columns = [
            'Loan Funded Date', 'Original Loan Due Date', 'Current Loan Due Date', 'Builders Risk Insurance Expiration'
        ]

        percentage_columns = [
            'LTV', 'CLTV', 'LTC', 'As Permitted LTV', 'As Permitted CLTV', 'Current Interest Rate', '% DISB'
        ]

        numeric_columns = [
            'Appraised Value', 'Collateral Holdback', 'As Permitted Property Value', 'Current Loan Amount', 'Total Principal Loan Paydown', 'Total Interest Reserve Amount Disbursed', 'Interest Reserve Balance', 'Loan amount Disbursed excluding Interest Reserve', 'Balance to finish Including Retainage', 'Loan Amount Disbursed'
        ]


        # Removing Special Characters in Percentage columns
        for column in columns_to_clean + percentage_columns + numeric_columns:
            self.df[column] = self.df[column].str.replace("$", "").str.replace(",", "").str.replace("%", "").str.replace(" ", "").str.strip()

            # Convert to datetime
        for column in datetime_columns:
            self.df[column] = pd.to_datetime(self.df[column])

        # Convert to string
        for column in string_columns:
            self.df[column] = self.df[column].astype('string')

        for column in percentage_columns + numeric_columns:
            self.df[column] = self.df.apply(lambda x: self.to_decimal(x[column]), axis=1)
            

        super().transform_data()
