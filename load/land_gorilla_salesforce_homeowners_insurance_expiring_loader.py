from re import sub
import os, sys
parent_dir = os.path.abspath('..')
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from load.loader import Loader
import pandas as pd

class LandGorillaSalesforceHomeownersInsuranceExpirationLoader(Loader):
    def __init__(self, username:str, account:str, database:str, schema:str, warehouse:str, private_key:str, passphrase:str):
        super().__init__(username, account, database, schema, warehouse, private_key, passphrase)
    
    def write_table(self, dataframe: pd.DataFrame):
        # introspect to get the table name to avoid hard coding things
        class_name = self.__class__.__name__
        table = f"{self.snake_case(class_name)}".upper().removesuffix('_LOADER')
        super().write_table(dataframe, table)

    def snake_case(self, str:str):
        res = [str[0].lower()]
        for c in str[1:]:
            if c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
                res.append('_')
                res.append(c.lower())
            else:
                res.append(c)
        
        return ''.join(res)
