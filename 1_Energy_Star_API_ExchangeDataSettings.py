import xml.etree.ElementTree as Et
from EnergyStarAPI import EnergyStarClient
import array
import xlrd
import csv
import os
import smtplib
import time
import logging

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dataExchangeSettings.log')
logging.basicConfig(filename=filename, level=logging.DEBUG, filemode='w')

WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document.XLSX')
customFieldSetUp = WebServices_ReferenceDoc.sheet_by_index(2)
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
PMUserName = liveEnvironmentAccounts.cell_value(1,1)
PMPassword = liveEnvironmentAccounts.cell_value(1,2)

NumberofRows = customFieldSetUp.nrows
NumberofColumns = customFieldSetUp.ncols

ES_Client = EnergyStarClient(PMUserName,PMPassword)


i = 1
while i < NumberofRows:
	xmlFileName = customFieldSetUp.cell_value(i,0)
	commandToExe = customFieldSetUp.cell_value(i,1)
	rootName = Et.Element(customFieldSetUp.cell_value(i,2))
	b = 3
	
	while b <= 10:
		rootName.set(customFieldSetUp.cell_value(i,b),str(customFieldSetUp.cell_value(i,b+1)))
		b += 2
	b = 11
	while b < NumberofColumns:
		if customFieldSetUp.cell_value(i,b) == "N/A":
			break
		else:
			subElementX = Et.SubElement(rootName,customFieldSetUp.cell_value(i,b))
			subElementX.text = str(customFieldSetUp.cell_value(i,b+1))
			b += 2
	
	
	
	mydata = Et.tostring(rootName, short_empty_elements = True)
	myfile = open('xml-templates/'+xmlFileName +'.xml','wb')
	myfile.write(mydata)
	myfile.close()
	
	if customFieldSetUp.cell_value(i,1) == 'POST':
		with open('xml-templates/'+xmlFileName +'.xml') as template_file:
			ES_Client.post_custom_fields(template_file)
		
	if customFieldSetUp.cell_value(i,1) == 'PUT':
		dataforlink = ES_Client.get_custom_fields_list()
		root = Et.fromstring(dataforlink)
		for child in root.findall('links'):
			for child2 in child.findall('link'):
				if child2.get('hint') == customFieldSetUp.cell_value(i,8):
					customFieldID = child2.get('link')
					with open('xml-templates/'+xmlFileName +'.xml') as template_file:
						ES_Client.put_update_custom_fields(template_file,customFieldID)
		
	if customFieldSetUp.cell_value(i,1) == 'DELETE':
		dataforlink = ES_Client.get_custom_fields_list()
		root = Et.fromstring(dataforlink)
		for child in root.findall('links'):
			for child2 in child.findall('link'):
				if child2.get('hint') == customFieldSetUp.cell_value(i,8):
					customFieldID = child2.get('link')
					with open('xml-templates/'+xmlFileName +'.xml') as template_file:
						ES_Client.delete_custom_fields(customFieldID)
		
	if customFieldSetUp.cell_value(i,1) == 'NONE':
		print ('No Action')
	
	i += 1

