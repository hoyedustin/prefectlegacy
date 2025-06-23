
import os, sys
import pandas as pd 
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import requests

class Authenticator:
  def __init__(self, username, password):
    self.username = username
    self.password = password
    #self.api_token = None

  def get_token(self):
    GetTokenURL = "https://clmapi.landgorilla.com/api/token"
    body = {'api_name': 'clm'}
    headers = {
      'USER': self.username,
      'PASSWORD': self.password
    }

    try:
      response = requests.get(GetTokenURL, headers=headers, json=body)
      response.raise_for_status()

      json_data = response.json()
      self.api_token = json_data.get('token')

      if self.api_token:
        print(f"API Token: {self.api_token}")
      else:
        raise ValueError ("API Token Invalid")
      return self.api_token

    except requests.exceptions.RequestException as e:
      print(f"Request failed: {e}")
      return None

  def has_token(self):
    return self.api_token is not None
