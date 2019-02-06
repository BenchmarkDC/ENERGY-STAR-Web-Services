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

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'disconnect.log')
logging.basicConfig(filename=filename, level=logging.DEBUG, filemode='w')

 
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)


WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document.XLSX')
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
propertiesToReject = WebServices_ReferenceDoc.sheet_by_index(6)
PMUserName = liveEnvironmentAccounts.cell_value(1,1)
PMPassword = liveEnvironmentAccounts.cell_value(1,2)



### Web Services Login
ES_Client = EnergyStarClient(PMUserName,PMPassword)

### Pulls all connected accounts and properties
accountIDs =[]
propertyIDs = []
accountIDs = [propertiesToReject.cell_value(r, 0) for r in range(propertiesToReject.nrows)]
print (accountIDs)
i = 1
while i <len(accountIDs):
	print (accountIDs[i])
	with open('xml-templates/Account_Disconnection.xml') as template_file:
		ES_Client.post_disconnect(template_file, accountIDs[i])
	i += 1


print ("----Disconnects all properties and accounts")


