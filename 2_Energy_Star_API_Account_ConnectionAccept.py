import xml.etree.ElementTree as Et
from EnergyStarAPI import EnergyStarClient
from pyo365 import MSGraphProtocol, Connection, Account, Message
from validate_email import validate_email
from datetime import timedelta, datetime
from ast import literal_eval
import numpy as np
import pandas as pd
from email.mime.text import MIMEText
from O365 import MSGraphProtocol, Connection, Account
from O365.message import  Message
import array, xlrd, csv, os, time, logging, timeit, sys, json, requests





 
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
output_csvfile = "CSV output/contact info_" + tstamp +".csv"


filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'connectAccept.log')
logging.basicConfig(filename=filename, level=logging.DEBUG, filemode='w')

liveEnvironmentAccounts = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='liveEnvironmentAccounts', dtype = str)  
customFieldSetUp = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='customFieldSetUp', dtype = str)  
emailBodys = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='emailBodys', dtype = str) 
o365UserName = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='Office365','Account UserName'].values[0])
o365Password = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='Office365','Account Password'].values[0])
PMUserName = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='ENERGY STAR Account','Account UserName'].values[0])
PMPassword = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='ENERGY STAR Account','Account Password'].values[0])

### Account Login
ES_Client = EnergyStarClient(PMUserName,PMPassword)
credentials = (o365UserName, o365Password)
scopes = ["https://graph.microsoft.com/Mail.ReadWrite", "https://graph.microsoft.com/Mail.Send"]
account = Account(credentials = credentials, scopes = scopes)
account.connection.refresh_token

print ("----Gets Pending List IDs/Accepts invitation/Gets Custom Field Information")

accountIDs = ES_Client.get_pending_connection_list_multipage()
Data_View = pd.DataFrame(list(set(accountIDs)), columns =  ['Account IDs'])

for index, item in Data_View.iterrows():
	with open('xml-templates/connection.xml') as template_file:
		ES_Client.post_accept_invite(template_file, item['Account IDs'])
	
	customdata = ES_Client.get_custom_field_data(item['Account IDs']).text
	root = Et.fromstring(customdata)
	for item2 in customFieldSetUp['textAttribute0.3']:
		for Child in root.findall(".//*[@name='"+item2+"']"):
			Data_View.loc[index,item2] = Child.text
			print (Child.text)
			print (item2)

				
print (Data_View)

for index, item in Data_View.iterrows():
	is_valid = validate_email(item['Email Address'], verify = True)
	print (item['Email Address'])
	if is_valid != False:
		messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Account Share Accepted','Subject'].values[0]
		messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Account Share Accepted','Body'].values[0]
		messageBody = messageBody.replace('[NAME]', item['Contact Name'])
		m = account.new_message()
		m.sender.address = 'info.benchmark@DC.gov'
		m.to.add(item['Email Address'])
		m.subject = messageSubject
		m.body = messageBody
		m.send()		



