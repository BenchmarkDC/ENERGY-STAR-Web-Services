import xml.etree.ElementTree as Et
from EnergyStarAPI import EnergyStarClient
from pyo365 import MSGraphProtocol, Connection, Account, Message
from validate_email import validate_email
from datetime import timedelta, datetime
import array, xlrd, csv, os, time, logging, timeit


 
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

WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document.XLSX')
customFieldSetUp = WebServices_ReferenceDoc.sheet_by_index(2)
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
emailsToSend = WebServices_ReferenceDoc.sheet_by_index(7)
PMUserName = liveEnvironmentAccounts.cell_value(1,1)
PMPassword = liveEnvironmentAccounts.cell_value(1,2)
o365UserName = liveEnvironmentAccounts.cell_value(3,1)
o365Password = liveEnvironmentAccounts.cell_value(3,2)
numberofRows = customFieldSetUp.nrows

### Account Login
ES_Client = EnergyStarClient(PMUserName,PMPassword)
credentials = (o365UserName, o365Password)
scopes = ["https://graph.microsoft.com/Mail.ReadWrite", "https://graph.microsoft.com/Mail.Send"]
account = Account(credentials = credentials, scopes = scopes)

print ("----Gets Pending List IDs/Accepts invitation/Gets Custom Field Information")

accountIDs = ES_Client.get_pending_connection_list_multipage()

accountIDs = list(set(accountIDs))

print (accountIDs)

i = 0
while i < len(accountIDs):
	with open('xml-templates/connection.xml') as template_file:
		ES_Client.post_accept_invite(template_file, accountIDs[i])
	i += 1



customFieldValues = []
headers = []
i = 0
while i < len(accountIDs):
	headers = []
	headers.append('Account IDs')
	entrylist = []
	entrylist.append(accountIDs[i])
	customdata = ES_Client.get_custom_field_data(accountIDs[i]).text
	root = Et.fromstring(customdata)
	b = 1
	while b < numberofRows:
		for Child in root.findall(".//*[@name='"+customFieldSetUp.cell_value(b,8)+"']"):
			entrylist.append(Child.text)
			headers.append(customFieldSetUp.cell_value(b,8))
		b += 1
	customFieldValues.append(entrylist)

	i += 1


with open(output_csvfile,'w') as output:
	writer = csv.writer(output, lineterminator='\n')
	writer.writerow(headers)
	writer.writerows(customFieldValues)



contactName = []
contactEmail = []
account.connection.refresh_token

i = 0
with open(output_csvfile,newline='') as CL:
	reader = csv.DictReader(CL)
	for row in reader:
		contactName.append(row['Contact Name'])
		contactEmail.append(row['Email Address'])
		print (contactName, contactEmail)
	while i < len(contactEmail):
		messageSubject = emailsToSend.cell_value(1,1)
		messageBody = emailsToSend.cell_value(1,2)
		is_valid = validate_email(contactEmail[i], verify = True)
		print (is_valid)
		if is_valid != False:
			logging.warning('message will be sent')
			m = account.new_message()
			messageBody = messageBody.replace('[NAME]', contactName[i])
			m.sender.address = 'info.benchmark@DC.gov'
			m.to.add(contactEmail[i])
			m.subject = messageSubject
			m.body = messageBody
			m.send()
			i += 1
		else:
			i += 1
