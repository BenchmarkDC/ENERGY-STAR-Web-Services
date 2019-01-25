import xml.etree.ElementTree as Et
from EnergyStarAPI import EnergyStarClient
import array
import xlrd
import csv
import os
import smtplib
import time
from ast import literal_eval
import logging
from pyo365 import MSGraphProtocol, Connection, Account, Message

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'propertyConnectAccept.log')
logging.basicConfig(filename=filename, level=logging.DEBUG, filemode='w')

 
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)

createFolder('./CSV output/')
ts =  time.gmtime()
tstamp =  (time.strftime("%Y%m%d_%H%M%S", ts))
temp = "CSV output/temp.csv"
output_csvfile = "CSV output/contact info_" + tstamp +".csv"

WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document_20181218.XLSX')
customFieldSetUp = WebServices_ReferenceDoc.sheet_by_index(2)
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
DCBuildingList = WebServices_ReferenceDoc.sheet_by_index(4)
emailsToSend = WebServices_ReferenceDoc.sheet_by_index(7)
PMUserName = liveEnvironmentAccounts.cell_value(1,1)
PMPassword = liveEnvironmentAccounts.cell_value(1,2)
o365UserName = liveEnvironmentAccounts.cell_value(3,1)
o365Password = liveEnvironmentAccounts.cell_value(3,2)
numberofRows = customFieldSetUp.nrows
numberofCoveredBuildings = DCBuildingList.nrows
acceptMessageSubject = emailsToSend.cell_value(2,1)
acceptMessageBody = emailsToSend.cell_value(2,2)
rejectMessageSubject = emailsToSend.cell_value(2,1)
rejectMessageBody = emailsToSend.cell_value(2,2)

### Web Services Login
ES_Client = EnergyStarClient(PMUserName,PMPassword)
credentials = (o365UserName, o365Password)
account = Account(credentials = credentials)
account.connection.refresh_token

DCRealPropertyIDs =[]

i = 1
while i < numberofCoveredBuildings:
	value1 = DCBuildingList.cell_value(i,0)
	DCRealPropertyIDs.append(value1)
	i += 1

propertyIDstoAccept = ES_Client.get_pending_propertyconnection_list_multipage_accept(DCRealPropertyIDs)
propertyIDstoReject = ES_Client.get_pending_propertyconnection_list_multipage_reject(DCRealPropertyIDs)




print ("----Accepts/Rejects invitation")

i = 0
while i < len(propertyIDstoAccept):
	with open('xml-templates/property_accept_connection.xml') as template_file:
			ES_Client.post_property_response(template_file, propertyIDstoAccept[i])
	propertyContactName = ''.join(ES_Client.get_Property_ContactName(propertyIDstoAccept[i]))
	propertyContactEmail = ES_Client.get_Property_ContactEmail(propertyIDstoAccept[i])
	propertyDCRealID = ''.join(ES_Client.get_DCRealID(propertyIDstoAccept[i]))
	propertyAdditionalDCRealID = ''.join(ES_Client.get_Additional_DCRealID(propertyIDstoAccept[i]))
	#tempaccount = Et.fromstring(ES_Client.get_property_info()
	m = account.new_message()
	m.sender.address = 'info.benchmark@DC.gov'
	m.to.add(propertyContactEmail)
	m.subject = acceptMessageSubject
	m.body = 'Dear ' + propertyContactName + ',<br><br>' + acceptMessageBody + propertyDCRealID + propertyAdditionalDCRealID + '<br><br>Thank you, <br><br> Energy Benchmarking Program'
	m.send()
	
	i += 1

### Re-WORK THIS

i = 0
while i < len(propertyIDstoReject):
	propertyContactName = ''.join(ES_Client.get_Property_ContactName(propertyIDstoReject[i]))
	propertyContactEmail = ES_Client.get_Property_ContactEmail(propertyIDstoReject[i])
	propertyDCRealID = ''.join(ES_Client.get_DCRealID(propertyIDstoReject[i]))
	propertyAdditionalDCRealID = ''.join(ES_Client.get_Additional_DCRealID(propertyIDstoReject[i]))
	#tempaccount = Et.fromstring(ES_Client.get_property_info()
	m = account.new_message()
	m.sender.address = 'info.benchmark@DC.gov'
	m.to.add(propertyContactEmail)
	m.subject = rejectMessageSubject
	m.body = 'Dear ' + propertyContactName + ',<br><br>' + rejectMessageBody + propertyDCRealID + propertyAdditionalDCRealID + '<br><br>Thank you, <br><br> Energy Benchmarking Program'
	m.send()	
	with open('xml-templates/property_reject_connection.xml') as template_file:
			ES_Client.post_property_response(template_file, propertyIDstoReject[i])
	i += 1

accountIDs =[]
propertyIDs = []
accountIDs = ES_Client.get_customer_list()

i = 0
while i <len(accountIDs):
	propertyIDs.extend(ES_Client.get_property_list(accountIDs[i]))
	i += 1

meterIDstoAccept = ES_Client.get_pending_meterconnection_list_multipage_accept(propertyIDs)

i = 0
while i < len(meterIDstoAccept):
	with open('xml-templates/meter_accept_connection.xml') as template_file:
			ES_Client.post_meter_response(template_file, meterIDstoAccept[i])
	i += 1
