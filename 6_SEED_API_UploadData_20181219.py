import xml.etree.ElementTree as Et
from SEEDAPI import SEEDClient
import array
import xlrd
import csv
import os
import smtplib
import time
from ast import literal_eval
import logging
import requests
from pyseed import SEEDReadWriteClient

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'SEEDUpload.log')
logging.basicConfig(filename=filename, level=logging.DEBUG, filemode='w')


WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document_20181218.XLSX')
customFieldSetUp = WebServices_ReferenceDoc.sheet_by_index(2)
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
SEEDUserName = liveEnvironmentAccounts.cell_value(2,1)
SEEDPassword = liveEnvironmentAccounts.cell_value(2,2)
SEEDorgID = '7'
numberofRows = customFieldSetUp.nrows
SEEDURL = 'http://seeddemostaging.lbl.gov'
import_record = '1898'
#import_record = '1898'
file_name = 'testdata'
### SEED Login

SEED_Client = SEEDClient(SEEDUserName, SEEDPassword, SEEDorgID)
r = SEED_Client.get_organizations()
b = SEED_Client.get_cycles()
e = SEED_Client.get_datasets()

print (r.text)
print (b.text)
print (e.text)

with open('CSV output/contact info_20181226_220217.csv', 'rb') as f:
	payload = {'file': (file_name,f)}
	c = SEED_Client.post_newfile(import_record, payload)



print (c.text)

