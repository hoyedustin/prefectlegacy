import os, sys
parent_dir = os.path.abspath('..')
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from decimal import *
from transform.transformer import Transformer
import pandas as pd
import numpy as np

class LandGorillaLoanIDTransformer(Transformer):
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
            'Land Gorilla Loan ID', 'First Name', 'Last Name / Business'
        ]
    
        string_columns = [
            'Land Gorilla Loan ID', 'First Name', 'Last Name / Business'
        ]
    
    
    
        # clean columns
        for column in columns_to_clean:
            self.df[column] = self.df[column].astype(str).fillna('').str.replace("$", "").str.replace(",", "").str.replace("%", "").str.replace(" ", "").str.strip()

    
        # convert to string
        for column in string_columns:
            self.df[column] = self.df[column].astype('string')

        super().transform_data()
