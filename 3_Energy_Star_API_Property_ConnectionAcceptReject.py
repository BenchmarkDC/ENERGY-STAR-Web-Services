import xml.etree.ElementTree as Et
from EnergyStarAPI import EnergyStarClient
from pyo365 import MSGraphProtocol, Connection, Account, Message
from validate_email import validate_email
from datetime import timedelta, datetime
from ast import literal_eval
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz, process
from email.mime.text import MIMEText
from O365 import MSGraphProtocol, Connection, Account
from O365.message import  Message
import array, xlrd, csv, os, time, logging, timeit, sys, json, requests

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'propertyConnectAccept.log')
logging.basicConfig(filename=filename, level=logging.DEBUG, filemode='w')

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)
createFolder('./CSV output/')

temp = "CSV output/temp.csv"
date_format = "%Y%m%d_%H%M%S"
date_End_temp = datetime.now()
tstamp = date_End_temp.strftime(date_format)


liveEnvironmentAccounts = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='liveEnvironmentAccounts', dtype = str)  
customFieldSetUp = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='customFieldSetUp', dtype = str)  
emailBodys = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='emailBodys', dtype = str) 
DCBuildingList = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='buildingListCurrent', dtype = str) 
o365UserName = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='Office365','Account UserName'].values[0])
o365Password = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='Office365','Account Password'].values[0])
PMUserName = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='ENERGY STAR Account','Account UserName'].values[0])
PMPassword = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='ENERGY STAR Account','Account Password'].values[0])



### Web Services Login
ES_Client = EnergyStarClient(PMUserName,PMPassword)
credentials = (o365UserName, o365Password)
scopes = ["https://graph.microsoft.com/Mail.ReadWrite", "https://graph.microsoft.com/Mail.Send"]
account = Account(credentials = credentials, scopes = scopes)
account.connection.refresh_token

pendingPropertyShares = ES_Client.get_pending_propertyconnection_list_multipage()


for index, item in pendingPropertyShares.iterrows():
	"""
	choices = DCBuildingList['DC Real Property  Unique ID'].tolist()
	print (item['ReportedDCID'])
	ratio = process.extract(str(item['ReportedDCID']), choices)
	print (ratio[1])
	if ratio[1][1] >= 85:
	"""
	item['acccept/reject'] = 'accept'
	with open('xml-templates/property_accept_connection.xml') as template_file:
		ES_Client.post_property_response(template_file, item['PropertyID'])
	propertyContactName = ''.join(ES_Client.get_Property_ContactName(item['PropertyID']))
	propertyContactEmail = ''.join(ES_Client.get_Property_ContactEmail(item['PropertyID']))
	propertyDCRealID = ''.join(ES_Client.get_DCRealID(item['PropertyID']))
	propertyName = ''.join(ES_Client.get_property_name(item['PropertyID']))
	messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Property Share Accepted','Subject'].values[0]
	messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Property Share Accepted','Body'].values[0]
	is_valid = validate_email(propertyContactEmail, verify = True)
	if is_valid != False:
		messageBody = messageBody.replace('[NAME]', propertyContactName)
		messageBody = messageBody.replace('[PROPERTY NAME]', propertyName)
		messageBody = messageBody.replace('[UBI]', propertyDCRealID)
		m = account.new_message()
		m.sender.address = 'info.benchmark@DC.gov'
		m.cc.add('info.benchmark@DC.gov')	
		m.to.add(propertyContactEmail)
		m.subject = messageSubject
		m.body = messageBody
		m.send()
	
	"""
	elif ratio[1][1] < 85:
		item['acccept/reject'] = 'reject'
		with open('xml-templates/property_reject_connection.xml') as template_file:
			ES_Client.post_property_response(template_file, item['PropertyID'])
		propertyContactName = ''.join(ES_Client.get_Property_ContactName(item['PropertyID']))
		propertyContactEmail = ''.join(ES_Client.get_Property_ContactEmail(item['PropertyID']))
		propertyDCRealID = ''.join(ES_Client.get_DCRealID(item['PropertyID']))
		propertyName = ''.join(ES_Client.get_property_name(item['PropertyID']))
		messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Property Share Rejected','Subject'].values[0]
		messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Property Share Rejected','Body'].values[0]
		is_valid = validate_email(propertyContactEmail, verify = True)
		if is_valid != False:
			messageBody = messageBody.replace('[NAME]', propertyContactName)
			messageBody = messageBody.replace('[PROPERTY NAME]', propertyName)
			messageBody = messageBody.replace('[UBI]', propertyDCRealID)
			m = account.new_message()
			m.sender.address = 'info.benchmark@DC.gov'
			m.cc.add('info.benchmark@DC.gov')	
			m.to.add(propertyContactEmail)
			m.bcc.add('JoAnna.Saunders@dc.gov')
			m.subject = messageSubject
			m.body = messageBody
			m.send()
	"""

print ("----Accepts/Rejects invitation")



### Accepts all Meter Requests

accountIDs =[]
propertyIDs = []
accountIDs = ES_Client.get_customer_list()

i = 0
while i <len(accountIDs):
	try:
		propertyIDs.extend(ES_Client.get_property_list(accountIDs[i]))
		i += 1
	except:
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
