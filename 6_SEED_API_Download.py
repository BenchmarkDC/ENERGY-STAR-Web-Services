import xml.etree.ElementTree as Et
from SEEDAPI import SEEDClient
from ast import literal_eval
import requests, json, logging, time, smtplib, os, csv, xlrd, array

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'SEEDUpload.log')
logging.basicConfig(filename=filename, level=logging.DEBUG, filemode='w')


WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document_20181218.XLSX')
customFieldSetUp = WebServices_ReferenceDoc.sheet_by_index(2)
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
SEEDUserName = liveEnvironmentAccounts.cell_value(2,1)
SEEDPassword = liveEnvironmentAccounts.cell_value(2,2)
numberofRows = customFieldSetUp.nrows
### SEED Login

SEED_Client = SEEDClient(SEEDUserName, SEEDPassword, SEEDorgID)



## Downloads latest cycle of data with labels
data_View = json.loads(SEED_Client.post_property_list(SEEDcycleID).text)
SEED_Property_Ids = []
label_Lookup_temp1 = []
label_Lookup_temp2 = []

for item in data_View['results']:
	SEED_Property_Ids.append(item['property_view_id'])

SEED_Label_List = json.loads(SEED_Client.post_property_filter_list().text)

for item in SEED_Label_List:
	label_Lookup_temp1.append(item['id'])
	label_Lookup_temp2.append(item['name'])

label_Lookup = dict(zip(label_Lookup_temp1,label_Lookup_temp2))


i = 0
while i < len(SEED_Property_Ids):
	print (SEED_Property_Ids[i])
	SEED_Property_Record = json.loads(SEED_Client.get_property_info(str(SEED_Property_Ids[i])).text)
	SEED_Property_Label_temp = SEED_Property_Record['property']['labels']
	SEED_Property_Label = [label_Lookup.get(item,item) for item in SEED_Property_Label_temp]
	### What additional information do we need to pull to get tool to work?
	### GET THIS TO EXPORT TO CSV AND EMAIL COPY TO INFO.BENCHMARK
	i += 1




