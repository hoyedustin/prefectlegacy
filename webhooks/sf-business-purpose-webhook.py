import requests
import json
from datetime import datetime
from prefect import task, flow
import os
from postmarker.core import PostmarkClient
from prefect.blocks.system import Secret
import xml.etree.ElementTree as ET
github_access_token = Secret.load("github-access-token")
dustin_sf_un = Secret.load("dustin-salesforce-un")
dustin_sf_pw = Secret.load("dustin-salesforce-pw")
dustin_sf_security_token = Secret.load("dustin-salesforce-security-token")
lg_pw = Secret.load("lg-api-pw")
lg_un = Secret.load("lg-api-un")
postmark_server_token = Secret.load("postmark-token")
#testing under this line for creation in SF
from simple_salesforce import Salesforce
sf = Salesforce(username=dustin_sf_un.get(), password=dustin_sf_pw.get(), security_token=dustin_sf_security_token.get(),instance= 'https://legacycapitalgroup.my.salesforce.com/lightning/o/Case/list?filterName=All_Servicing_Cases/')

@task
def parse_xml(XML_body):
    XML_body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n <soapenv:Body>\n  <notifications xmlns=\"http://soap.sforce.com/2005/09/outbound\">\n   <OrganizationId>00D8c000002JHMDEA4</OrganizationId>\n   <ActionId>04kRi0000061wbCIAQ</ActionId>\n   <SessionId xsi:nil=\"true\"/>\n   <EnterpriseUrl>https://legacycapitalgroup.my.salesforce.com/services/Soap/c/61.0/00D8c000002JHMD</EnterpriseUrl>\n   <PartnerUrl>https://legacycapitalgroup.my.salesforce.com/services/Soap/u/61.0/00D8c000002JHMD</PartnerUrl>\n   <Notification>\n    <Id>04lRi00000SEBI1IAP</Id>\n    <sObject xsi:type=\"sf:Case\" xmlns:sf=\"urn:sobject.enterprise.soap.sforce.com\">\n     <sf:Id>500Ri00000PThftIAD</sf:Id>\n     <sf:Borrower__c>Dodobara</sf:Borrower__c>\n     <sf:CaseNumber>00016492</sf:CaseNumber>\n     <sf:RecordTypeId>0128c000001eZ6FAAU</sf:RecordTypeId>\n     <sf:Status>New</sf:Status>\n    </sObject>\n   </Notification>\n  </notifications>\n </soapenv:Body>\n</soapenv:Envelope>"

    print(XML_body)
    
    namespaces = {
        'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'sf': 'urn:sobject.enterprise.soap.sforce.com',
        '': 'http://soap.sforce.com/2005/09/outbound'
    }
    
    root = ET.fromstring(XML_body)
    
    org_id = root.find('.//{http://soap.sforce.com/2005/09/outbound}OrganizationId').text
    action_id = root.find('.//{http://soap.sforce.com/2005/09/outbound}ActionId').text
    enterprise_url = root.find('.//{http://soap.sforce.com/2005/09/outbound}EnterpriseUrl').text
    partner_url = root.find('.//{http://soap.sforce.com/2005/09/outbound}PartnerUrl').text
    notification_id = root.find('.//{http://soap.sforce.com/2005/09/outbound}Notification/{http://soap.sforce.com/2005/09/outbound}Id').text
    case_id = root.find('.//{urn:sobject.enterprise.soap.sforce.com}Id').text
    borrower = root.find('.//{urn:sobject.enterprise.soap.sforce.com}Borrower__c').text
    case_number = root.find('.//{urn:sobject.enterprise.soap.sforce.com}CaseNumber').text
    record_type_id = root.find('.//{urn:sobject.enterprise.soap.sforce.com}RecordTypeId').text
    status = root.find('.//{urn:sobject.enterprise.soap.sforce.com}Status').text
    
    print(f'Organization ID: {org_id}')
    print(f'Action ID: {action_id}')
    print(f'Enterprise URL: {enterprise_url}')
    print(f'Partner URL: {partner_url}')
    print(f'Notification ID: {notification_id}')
    print(f'Case ID: {case_id}')
    print(f'Borrower: {borrower}')
    print(f'Case Number: {case_number}')
    print(f'Record Type ID: {record_type_id}')
    print(f'Status: {status}')

    return org_id, action_id, enterprise_url, partner_url, notification_id, case_id, borrower, case_number, record_type_id, status


@task
def send_email(org_id, action_id, enterprise_url, partner_url, notification_id, case_id, borrower, case_number, record_type_id, status):
    postmark_server_token_prefect = postmark_server_token.get()
    postmark = PostmarkClient (server_token= postmark_server_token_prefect)
    postmark.emails.send(
    From= 'legacy-info@rook.capital',
    To='everetth@legacyg.com',
    Subject= f" SF CASE: {case_number}",
    HtmlBody= f'Organization ID: {org_id}'
    f'Action ID: {action_id}'
    f'Enterprise URL: {enterprise_url}'
    f'Partner URL: {partner_url}'
    f'Notification ID: {notification_id}'
    f'Case ID: {case_id}'
    f'Borrower: {borrower}'
    f'Case Number: {case_number}'
    f'Record Type ID: {record_type_id}'
    f'Status: {status}'
    )


@flow (log_prints= True)
def sf_business_purpose_webhook(XML_body: str) -> None:
    org_id, action_id, enterprise_url, partner_url, notification_id, case_id, borrower, case_number, record_type_id, status = parse_xml(XML_body)
    send_email(org_id, action_id, enterprise_url, partner_url, notification_id, case_id, borrower, case_number, record_type_id, status)
