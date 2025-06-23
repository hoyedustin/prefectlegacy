import os, sys
parent_dir = os.path.abspath('..')
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from decimal import *
from transform.transformer import Transformer
import pandas as pd
import numpy as np

class LandGorillaPortfolioReportFullPopulationTransformer(Transformer):
    def __init__(self, df:pd.DataFrame):
        super().__init__(df)

    def to_decimal(self, x):
        try:
            if x and x == x and len(x) > 0:
                return Decimal(x) 
        except (InvalidOperation, ValueError):
            return np.nan
    
    def transform_data(self)-> None:
        # dropping rows that contain only null values and nothing else in order to get rid of the totals at the bottom
        self.df = self.df.dropna(subset = ['Loan Number'], how = 'all')

        def to_decimal(self, x):
            try:
                if x and x == x and len(x) > 0:
                    return Decimal(x) 
            except (InvalidOperation, ValueError):
                return np.nan
    
        columns_to_clean = [
            'Loan Originator', 'Loan Number', 'Borrower Last Name', 'Property Address', 'Investor', 'Loan Program', 'Loan Term in Months', 'Loan Funded Date', 'Original Loan Due Date', 
            'Current Loan Due Date', 'Current Loan Amount', 'Total Principal Loan Paydown', 'Balance to finish Including Retainage', '% DISB', 'Total Units',
            'County', 'Exception', 'Appraised Value', 'State', 'Current Loan Status', 'Additional Collateral Property', 'Funding Account', 'Outside Equity', 'LTV', 'CLTV', 'LTC', 'As Permitted LTV',
            'As Permitted CLTV', 'Current Interest Rate', 'Zip', 'Total Interest Reserve Amount Disbursed', 'Interest Reserve Balance', 'Collateral Holdback', 'As Permitted Property Value'
        ]
    
        string_columns = [
            'Borrower Last Name', 'Loan Number', 'Loan Originator', 'Property Address', 'Loan Program', 'Loan Term in Months', 'Total Units', 'Investor', 'County', 'Exception', 'State', 'Current Loan Status',
            'Additional Collateral Property', 'Funding Account', 'Outside Equity', 'Zip'
        ]
    
        datetime_columns = [
            'Loan Funded Date', 'Current Loan Due Date', 'Original Loan Due Date'
        ]

        date_columns = [
            'Loan Status Completed Date (Most Recent Change)'
        ]
    
        percentage_columns = [
            '% DISB', 'LTV', 'CLTV', 'LTC', 'As Permitted LTV', 'As Permitted CLTV', 'Current Interest Rate'
        ]
    
        numeric_columns = [
            'Current Loan Amount', 'Total Principal Loan Paydown', 'Balance to finish Including Retainage','Appraised Value', 'Total Interest Reserve Amount Disbursed', 'Interest Reserve Balance',
            'Collateral Holdback', 'As Permitted Property Value'
        ]
    
        # clean columns
        for column in columns_to_clean:
            self.df[column] = self.df[column].astype(str).fillna('').str.replace("$", "").str.replace(",", "").str.replace("%", "").str.replace(" ", "").str.strip()
    
        # convert to datetime
        for column in datetime_columns:
            self.df[column] = pd.to_datetime(self.df[column])

        # convert to date
        for column in date_columns:
            self.df[column] = pd.to_datetime(self.df[column]).dt.date
    
        # convert to string
        for column in string_columns:
            self.df[column] = self.df[column].astype('string')
    
        # convert to decimal
        for column in numeric_columns + percentage_columns:
            self.df[column] = self.df[column].apply(lambda x: self.to_decimal(x))

        super().transform_data()
