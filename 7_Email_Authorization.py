import xml.etree.ElementTree as Et
import array
import xlrd
import csv
import os
from pyo365 import MSGraphProtocol, Connection, Account, Message, oauth_authentication_flow
import time
import logging
import timeit

WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document_20181218.XLSX')
customFieldSetUp = WebServices_ReferenceDoc.sheet_by_index(2)
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
emailsToSend = WebServices_ReferenceDoc.sheet_by_index(7)
o365UserName = liveEnvironmentAccounts.cell_value(3,1)
o365Password = liveEnvironmentAccounts.cell_value(3,2)


credentials = (o365UserName, o365Password)
scopes = ["https://graph.microsoft.com/Mail.ReadWrite", "https://graph.microsoft.com/Mail.Send", "https://graph.microsoft.com/User.Read", "offline_access"]
account = Connection(credentials = credentials, scopes = scopes)
url = account.get_authorization_url()
print (url)
result_url = input('Paste the resulting url here')
account.request_token(result_url)

#account = Account(credentials = credentials)
#result = oauth_authentication_flow(o365UserName,o365Password,scopes)
