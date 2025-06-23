import os, sys
parent_dir = os.path.abspath('..')
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from transform.transformer import Transformer
import pandas as pd

class LandGorillaMaturityDateTransformer(Transformer):
    def __init__(self, df:pd.DataFrame):
        super().__init__(df)

    def to_decimal(self, x):
        if x and x == x and len(x) > 0:
            return Decimal(x)
    
    def transform_data(self)-> None:
        # dropping rows that contain only null values and nothing else in order to get rid of the totals at the bottom
        self.df = self.df.dropna(subset = ['Loan Number'], how = 'all')

        # removing dollar signs and commas from numeric columns to convert them to numeric
        #self.df['Current Loan Amount'] = self.df['Current Loan Amount'].str.replace("$", "")
        #self.df['Current Loan Amount'] = self.df['Current Loan Amount'].str.replace(",", "")
        #self.df['Total Principal Loan Paydown'] = self.df['Total Principal Loan Paydown'].str.replace("$", "")
        #self.df['Total Principal Loan Paydown'] = self.df['Total Principal Loan Paydown'].str.replace(",", "")
        #self.df['Current Interest Rate'] = self.df['Current Interest Rate'].str.replace("%", "")
        
        # stripping spaces from numeric columns to change them to numeric
        #self.df['Current Loan Amount'] = self.df['Current Loan Amount'].str.replace(" ", "")
        #self.df['Current Loan Amount'] = self.df['Current Loan Amount'].str.strip()
        #self.df['Total Principal Loan Paydown'] = self.df['Total Principal Loan Paydown'].str.strip()
        
        #changing datatypes to string
        self.df['Loan Number'] = self.df['Loan Number'].astype('string')
        self.df['Borrower'] = self.df['Borrower'].astype('string')
        self.df['Property Address'] = self.df['Property Address'].astype('string')
        self.df['Loan Program'] = self.df['Loan Program'].astype('string')
        #self.df['Loan Term in Months'] = self.df['Loan Term in Months'].astype('string')
        self.df['Pinned Note'] = self.df['Pinned Note'].astype('string')
        
        
        #changing datatypes to numeric
        #self.df['Current Loan Amount'] = self.df['Current Loan Amount'].astype('float64')
        # self.df['Total Principal Loan Paydown'] = self.df['Total Principal Loan Paydown'].astype('float64')
        # self.df['Current Interest Rate'] = self.df['Current Interest Rate'].astype('float64')
        
        #changing datatypes to datetime
        self.df['Loan Funded Date'] = pd.to_datetime(self.df['Loan Funded Date'])
        self.df['Current Loan Due Date'] = pd.to_datetime(self.df['Current Loan Due Date'])

        super().transform_data()
