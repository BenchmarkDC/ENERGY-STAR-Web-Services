import xml.etree.ElementTree as Et
import array
import xlrd
import csv
import os
from pyo365 import MSGraphProtocol, Connection, Account, Message
import time
import logging
import timeit, fileinput

WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document_20181218.XLSX')
customFieldSetUp = WebServices_ReferenceDoc.sheet_by_index(2)
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
emailsToSend = WebServices_ReferenceDoc.sheet_by_index(7)
o365UserName = liveEnvironmentAccounts.cell_value(3,1)
o365Password = liveEnvironmentAccounts.cell_value(3,2)

credentials = (o365UserName, o365Password)
messageSubject = emailsToSend.cell_value(3,1)
messageBody = emailsToSend.cell_value(3,2)
Connection.refresh_token
client = Account(credentials)
m = client.new_message()
m.sender.address = 'info.benchmark@DC.gov'
m.to.add('Andrew.Held@dc.gov')
	
messageBody = messageBody.replace('[NAME]', 'Andrew')


m.subject = messageSubject
m.body = messageBody
m.send()
