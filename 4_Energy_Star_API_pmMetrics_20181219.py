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

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'metricsPMReports.log')
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
output_csvfile = "CSV output\metricsPMReports_" + tstamp +".csv"

WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document_20181218.XLSX')
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
pmMetricsToPull = WebServices_ReferenceDoc.sheet_by_index(5)
PMUserName = liveEnvironmentAccounts.cell_value(1,1)
PMPassword = liveEnvironmentAccounts.cell_value(1,2)



### Web Services Login
ES_Client = EnergyStarClient(PMUserName,PMPassword)

### Pulls all connected accounts and properties
accountIDs =[]
propertyIDs = []
DCRealPropertyIDs = []
accountIDs = ES_Client.get_customer_list()

i = 0
while i <len(accountIDs):
	propertyIDs.extend(ES_Client.get_property_list(accountIDs[i]))
	i += 1
i=0
while i <len(propertyIDs):
	DCRealPropertyIDs.extend(ES_Client.get_DCRealID(propertyIDs[i]))
	i += 1


print ("----Pulls all property Use IDs and Metrics")


year = '2017'
month = '12'
measurements = 'EPA'


NumberofRows_metricsPM = pmMetricsToPull.nrows
metrictopull = [pmMetricsToPull.cell_value(r, 5) for r in range(pmMetricsToPull.nrows)]
metrictopull = [str(item) for item in metrictopull]

yeartoPull = pmMetricsToPull.cell_value(1, 6)
numberOfMonths	= pmMetricsToPull.cell_value(1, 7)
unitsOfMeasurment= pmMetricsToPull.cell_value(1, 8)


del metrictopull[0]
metrictopull.insert(0, "DCRealPropertyID_Reported")
metrictopull.insert(0, "PropertyID")

metrictopull.extend(['Elec – Dec', 'Elec – Nov', 'Elec – Oct', 'Elec – Sept', 'Elec – Aug', 'Elec – July', 'Elec – June', 'Elec – May', 'Elec – Apr', 'Elec – March', 'Elec – Feb', 'Elec – Jan'])
metrictopull.extend(['NG – Dec', 'NG – Nov', 'NG – Oct', 'NG – Sept', 'NG – Aug', 'NG – July', 'NG – June', 'NG – May', 'NG – Apr', 'NG – March', 'NG – Feb', 'NG – Jan'])

with open(output_csvfile,'w',newline='') as output:
	i = len(metrictopull)
	writer = csv.writer(output,  delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	writer.writerow(metrictopull[0:i])



del metrictopull[0]
del metrictopull[0]
target_ibdex = metrictopull.index('Elec – Dec')
metrictopull = metrictopull[:target_ibdex]
DCRealPropertyIDs = tuple("=\"" + r + "\"" for r in DCRealPropertyIDs)

#### Update to pull Reason for No Score

i = 0
while i < len(propertyIDs):
	b = 0
	metricpulled = []
	while b < NumberofRows_metricsPM:
		t = metrictopull[b:9+b]
		y = ', '.join(t)
		roottemp =  ES_Client.get_metrics(propertyIDs[i], yeartoPull, numberOfMonths, unitsOfMeasurment, y).text
		root = Et.fromstring(roottemp.encode('utf-8'))
		for child in root.findall('metric'):
			value =  child.find('value').text
			metricpulled.append(value)	
		b += 9
	roottemp2 = ES_Client.get_montly_metrics(propertyIDs[i], yeartoPull, numberOfMonths, unitsOfMeasurment, "siteElectricityUseMonthly, siteNaturalGasUseMonthly").text
	root2 = Et.fromstring(roottemp2.encode('utf-8'))
	for child2 in root2.findall('metric'):
		for child3 in child2.findall('monthlyMetric'):
			value2 =  child3.find('value').text
			metricpulled.append(value2)	
	metricpulled.insert(0,DCRealPropertyIDs[i])
	metricpulled.insert(0,propertyIDs[i])
	with open(output_csvfile,'a',newline='') as output:
		writer = csv.writer(output,  delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
		writer.writerow(metricpulled)
	i += 1

