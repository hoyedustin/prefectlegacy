import pandas as pd
import numpy as np
from prefect import task, flow

class Transformer:
    def __init__(self, df:pd.DataFrame):
        self.df = df

    def transform_data(self):
        # changing all blank values to NaN
        self.df = self.df.replace({None: np.nan})

        # rename column names
        self.df = self.df.rename(columns={
                'Additional Collateral Property':'additional_collateral_property',
                'Appraised Value':'appraised_value',
                'As Permitted Property Value':'as_permitted_property_value',
                'As Permitted LTV':'as_permitted_ltv',
                'As Permitted CLTV':'as_permitted_cltv',
                'Balance to finish Including Retainage':'balance_to_finish_including_retainage',
                'Borrower': 'borrower',
                'Borrower Full Name': 'borrower_full_name',
                'Borrower Last Name': 'borrower_last_name',
                'Borrower Phone Number': 'borrower_phone_number',
                'Builders Risk Insurance Expiration': 'builders_risk_insurance_expiration',
                'Calculated LTV':'calculated_ltv',
                'City':'city',
                'CLTV':'cltv',
                'Co-Borrower 1 Full Name':'coborrower_1_full_name',
                'Collateral Holdback':'collateral_holdback',
                'County':'county',
                'Current Loan Amount': 'current_loan_amount',
                'Current Loan Due Date': 'current_loan_due_date',
                'Current Loan Status': 'current_loan_status',
                'Current Interest Rate': 'current_interest_rate',
                'Estimated Payoff Date':'estimated_payoff_date',
                'Exception':'exception',
                'File Number':'file_number',
                'First Name':'first_name',
                'Funding Account':'funding_account',
                'Interest Reserve Balance':'interest_reserve_balance',
                'Investor':'investor',
                'Labels':'labels',
                'Land Gorilla Loan ID':'land_gorilla_loan_id',
                'Last Inspection Complete':'last_inspection_complete',
                'Last Name / Business':'last_name_and_or_business',
                'Loan Amount Disbursed':'loan_amount_disbursed',
                'Loan amount Disbursed excluding Interest Reserve':'loan_amount_disbursed_excluding_interest_reserve',
                'Loan Funded Date': 'loan_funded_date',
                'Loan Originator': 'loan_originator',
                'Loan Number': 'loan_number',
                'Loan Program': 'loan_program', 
                'Loan Term in Months': 'loan_term_in_months',
                'Loan Status Completed Date (Most Recent Change)':'loan_status_completed_date_most_recent_change',
                'LTC':'ltc',
                'LTV':'ltv',
                'Original Loan Due Date':'original_loan_due_date',
                'Outside Equity':'outside_equity',
                'Pinned Note': 'pinned_note',
                'Property Address': 'property_address',
                'Risk Label': 'risk_label', 
                'Total Principal Loan Paydown': 'total_principal_loan_paydown', 
                'Total Units':'total_units',
                'Total Interest Reserve Amount Disbursed':'total_interest_reserve_amount_disbursed',
                'State':'state',
                'Zip':'zip',
                '% DISB':'%_disb',
            


                

            })
        return
