import xml.etree.ElementTree as Et
from EnergyStarAPI import EnergyStarClient
from ast import literal_eval
from pyo365 import MSGraphProtocol, Connection, Account, Message
from validate_email import validate_email
import array, xlrd, csv, os, smtplib, time, logging

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'propertyConnectAccept.log')
logging.basicConfig(filename=filename, level=logging.DEBUG, filemode='w')



WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document.XLSX')
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

propertyIDstoAccept = list(set(propertyIDstoAccept))
propertyIDstoReject = list(set(propertyIDstoReject))

print ("----Accepts/Rejects invitation")

i = 0
while i < len(propertyIDstoAccept):
	with open('xml-templates/property_accept_connection.xml') as template_file:
			ES_Client.post_property_response(template_file, propertyIDstoAccept[i])
	propertyContactName = ''.join(ES_Client.get_Property_ContactName(propertyIDstoAccept[i]))
	propertyContactEmail = ''.join(ES_Client.get_Property_ContactEmail(propertyIDstoAccept[i]))
	propertyDCRealID = ''.join(ES_Client.get_DCRealID(propertyIDstoAccept[i]))
	propertyName = ''.join(ES_Client.get_property_name(propertyIDstoAccept[i]))
	acceptMessageSubject = emailsToSend.cell_value(2,1)
	is_valid = validate_email(propertyContactEmail, verify = True)
	if is_valid == True:
		acceptMessageBody = emailsToSend.cell_value(2,2)
		acceptMessageBody = acceptMessageBody.replace('[NAME]', propertyContactName)
		acceptMessageBody = acceptMessageBody.replace('[PROPERTY NAME]', propertyName)
		acceptMessageBody = acceptMessageBody.replace('[UBI]', propertyDCRealID)
		m = account.new_message()
		m.sender.address = 'info.benchmark@DC.gov'
		m.to.add(propertyContactEmail)
		m.subject = acceptMessageSubject
		m.body = acceptMessageBody
		m.send()
		i += 1
	else:
		i += 1


i = 0
while i < len(propertyIDstoReject):
	with open('xml-templates/property_reject_connection.xml') as template_file:
		ES_Client.post_property_response(template_file, propertyIDstoReject[i])
	propertyContactName = ''.join(ES_Client.get_Property_ContactName(propertyIDstoReject[i]))
	propertyContactEmail = ''.join(ES_Client.get_Property_ContactEmail(propertyIDstoReject[i]))
	propertyDCRealID = ''.join(ES_Client.get_DCRealID(propertyIDstoReject[i]))
	propertyName = ''.join(ES_Client.get_property_name(propertyIDstoReject[i]))
	is_valid = validate_email(propertyContactEmail, verify = True)
	if is_valid == True:
		rejectMessageSubject = emailsToSend.cell_value(3,1)
		rejectMessageBody = emailsToSend.cell_value(3,2)
		rejectMessageBody = rejectMessageBody.replace('[NAME]', propertyContactName)
		rejectMessageBody = rejectMessageBody.replace('[PROPERTY NAME]', propertyName)
		rejectMessageBody = rejectMessageBody.replace('[UBI]', propertyDCRealID)
		m = account.new_message()
		m.sender.address = 'info.benchmark@DC.gov'
		m.to.add(propertyContactEmail)
		m.subject = rejectMessageSubject
		m.body = rejectMessageBody
		m.send()
		with open('xml-templates/property_disconnection.xml') as template_file:
			ES_Client.post_property_disconnect(template_file, propertyIDstoReject[i])
		i +=1
	else:

		with open('xml-templates/property_disconnection.xml') as template_file:
			ES_Client.post_property_disconnect(template_file, propertyIDstoReject[i])
		i +=1	


### Accepts all Meter Requests

accountIDs =[]
propertyIDs = []
accountIDs = ES_Client.get_customer_list()

i = 0
while i <len(accountIDs):
	propertyIDs.extend(ES_Client.get_property_list(accountIDs[i]))
	i += 1

meterIDstoAccept = ES_Client.get_pending_meterconnection_list_multipage_accept(propertyIDs)

meterIDstoAccept = list(set(meterIDstoAccept))

i = 0
while i < len(meterIDstoAccept):
	with open('xml-templates/meter_accept_connection.xml') as template_file:
			ES_Client.post_meter_response(template_file, meterIDstoAccept[i])
	i += 1


meterIDstoReject = ES_Client.get_pending_meterconnection_list_multipage_reject()

meterIDstoReject = list(set(meterIDstoReject))

i = 0
while i < len(meterIDstoReject):
	with open('xml-templates/meter_reject_connection.xml') as template_file:
			ES_Client.post_meter_response(template_file, meterIDstoReject[i])
	i += 1
