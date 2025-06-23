import os, sys
parent_dir = os.path.abspath('..')
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from decimal import *
from transform.transformer import Transformer
import pandas as pd
import numpy as np

class LandGorillaWatchlistTransformer(Transformer):
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
            'Loan Originator', 'Loan Number', 'Borrower Full Name', 'Co-Borrower 1 Full Name', 'Property Address', 'City', 'Investor', 'Loan Program', 'Loan Term in Months', 'Loan Funded Date', 'Original Loan Due Date', 
            'Current Loan Due Date', 'Current Loan Amount', 'Loan amount Disbursed excluding Interest Reserve', 'Total Principal Loan Paydown', 'Estimated Payoff Date', 'Balance to finish Including Retainage', '% DISB', 'Total Units',
            'Last Inspection Complete', 'Pinned Note', 'Labels'
        ]
    
        string_columns = [
            'Borrower Full Name', 'Loan Number', 'Loan Originator', 'Co-Borrower 1 Full Name', 'Property Address', 'Loan Program', 'Loan Term in Months', 'Pinned Note', 'Total Units', 'City', 'Investor','Labels'
        ]
    
        datetime_columns = [
            'Loan Funded Date', 'Estimated Payoff Date', 'Current Loan Due Date', 'Original Loan Due Date'
        ]
    
        percentage_columns = [
            'Last Inspection Complete', '% DISB'
        ]
    
        numeric_columns = [
            'Current Loan Amount', 'Total Principal Loan Paydown', 'Loan amount Disbursed excluding Interest Reserve', 'Balance to finish Including Retainage'
        ]
    
        # clean columns
        for column in columns_to_clean:
            self.df[column] = self.df[column].astype(str).fillna('').str.replace("$", "").str.replace(",", "").str.replace("%", "").str.replace(" ", "").str.strip()
    
        # convert to datetime
        for column in datetime_columns:
            self.df[column] = pd.to_datetime(self.df[column])
    
        # convert to string
        for column in string_columns:
            self.df[column] = self.df[column].astype('string')
    
        # convert to decimal
        for column in numeric_columns + percentage_columns:
            self.df[column] = self.df[column].apply(lambda x: self.to_decimal(x))

        super().transform_data()
