import xml.etree.ElementTree as Et
from EnergyStarAPI import EnergyStarClient
from SEEDAPI import SEEDClient, SEEDClientv2_1
import array, xlrd, csv, os, smtplib, time, logging, json
from ast import literal_eval

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
tstamp2 = (time.strftime("%m/%d/%y %H:%M:%S %p", ts))
output_csvfile = "CSV output\webServicesMetricsPMReports_" + tstamp +".csv"

WebServices_ReferenceDoc = xlrd.open_workbook('WebServices_Reference_Document.XLSX')
liveEnvironmentAccounts = WebServices_ReferenceDoc.sheet_by_index(3)
pmMetricsToPull = WebServices_ReferenceDoc.sheet_by_index(5)
PMUserName = liveEnvironmentAccounts.cell_value(1,1)
PMPassword = liveEnvironmentAccounts.cell_value(1,2)
SEEDUserName = liveEnvironmentAccounts.cell_value(2,1)
SEEDPassword = liveEnvironmentAccounts.cell_value(2,2)
SEEDorgID = liveEnvironmentAccounts.cell_value(2,3)
SEEDcycleID = liveEnvironmentAccounts.cell_value(2,4)
SEEDimportRecord = liveEnvironmentAccounts.cell_value(2,5)
SEEDtoPMreportID = liveEnvironmentAccounts.cell_value(2,6)

file_name = 'webServicesMetricsPMReports_' + tstamp +".csv"

### Web Services Login
ES_Client = EnergyStarClient(PMUserName,PMPassword)
SEED_Client = SEEDClient(SEEDUserName, SEEDPassword, SEEDorgID)
SEED_Client_v2_1 = SEEDClientv2_1(SEEDUserName, SEEDPassword, SEEDorgID)




### Pulls all connected accounts and properties
accountIDs =[]
propertyIDs = []
DCRealPropertyIDs_Custom = []
DCRealPropertyIDs_CustomAdditional = []
propertyName = []
address1 = []
address2 = []
city = []
county = []
state_Province = []
postal_Code = []
country  = []
primaryPropertyType_SelfSelected = []
construction_Status = []
numberOfBuildings = []
year_Built = []
occupancy = []
property_Notes = []
irrigated_Area = []
propertyContactName_Custom = []
propertyContactEmail_Custom = []

accountIDs = ES_Client.get_customer_list()

i = 0
while i <len(accountIDs):
	propertyIDs.extend(ES_Client.get_property_list(accountIDs[i]))
	i += 1


## Gets all property level details
# GET CUSTOM CONTACT INFORMATION HERE TOO!!!!!! THIS SHOULD BE ON BEHALF OF 
i=0
while i <len(propertyIDs):
	DCRealPropertyIDs_Custom.append(ES_Client.get_DCRealID(propertyIDs[i]))
	DCRealPropertyIDs_CustomAdditional.append(ES_Client.get_Additional_DCRealID(propertyIDs[i]))
	propertyName.append(ES_Client.get_property_name(propertyIDs[i]))
	address1.append(ES_Client.get_property_address1(propertyIDs[i]))
	address2.append(ES_Client.get_property_address2(propertyIDs[i]))
	city.append(ES_Client.get_property_city(propertyIDs[i]))
	state_Province.append(ES_Client.get_property_state(propertyIDs[i]))
	postal_Code.append(ES_Client.get_property_postal_code(propertyIDs[i]))
	country.append(ES_Client.get_property_country(propertyIDs[i]))
	primaryPropertyType_SelfSelected.append(ES_Client.get_property_type_self_selected(propertyIDs[i]))
	construction_Status.append(ES_Client.get_property_construction_status(propertyIDs[i]))
	numberOfBuildings.append(ES_Client.get_property_number_of_buildings(propertyIDs[i]))
	year_Built.append(ES_Client.get_property_year_built(propertyIDs[i]))
	occupancy.append(ES_Client.get_property_occupancy(propertyIDs[i]))
	property_Notes.append(ES_Client.get_property_notes(propertyIDs[i]))
	irrigated_Area.append(ES_Client.get_property_irrigated_area(propertyIDs[i]))
	propertyContactName_Custom.append((ES_Client.get_Property_ContactName(propertyIDs[i])))
	propertyContactEmail_Custom.append(ES_Client.get_Property_ContactEmail(propertyIDs[i]))
	i += 1


print ("----Pulls all property Use IDs and Metrics")

NumberofRows_metricsPM = pmMetricsToPull.nrows
metrictopull = [pmMetricsToPull.cell_value(r, 5) for r in range(pmMetricsToPull.nrows)]
metrictopull = [str(item) for item in metrictopull]

yeartoPull = pmMetricsToPull.cell_value(1, 6)
numberOfMonths	= pmMetricsToPull.cell_value(1, 7)
unitsOfMeasurment= pmMetricsToPull.cell_value(1, 8)

del metrictopull[0]
metrictopull.insert(0, "Metered Areas (Energy)")
metrictopull.insert(0, "Metered Areas (Water)")
metrictopull.insert(0, "LEED US Project ID")
metrictopull.insert(0, "CoStar Property ID")
metrictopull.insert(0, "Custom Property ID 1 - ID")
metrictopull.insert(0, "District of Columbia Building Unique ID")
metrictopull.insert(0, "District of Columbia Real Property Unique ID")
metrictopull.insert(0, "Web Services Pull Date")
metrictopull.insert(0, "propertyContactName_Custom")
metrictopull.insert(0, "propertyContactEmail_Custom")
metrictopull.insert(0, "Property Notes")
metrictopull.insert(0, "Irrigated Area")
metrictopull.insert(0, "Occupancy")
metrictopull.insert(0, "Year Built")
metrictopull.insert(0, "Number of Buildings")
metrictopull.insert(0, "Construction Status")
metrictopull.insert(0, "Primary Property Type - Self Selected")
metrictopull.insert(0, "Country")
metrictopull.insert(0, "Postal Code")
metrictopull.insert(0, "State/Province")
metrictopull.insert(0, "City")
metrictopull.insert(0, "Address 2")
metrictopull.insert(0, "Address 1")
metrictopull.insert(0, "Property Name")
metrictopull.insert(0, "DCRealPropertyID_CustomAdditional")
metrictopull.insert(0, "DCRealPropertyID_Custom")
metrictopull.insert(0, "PropertyID")




metrictopull.extend(['Elec – Dec', 'Elec – Nov', 'Elec – Oct', 'Elec – Sept', 'Elec – Aug', 'Elec – July', 'Elec – June', 'Elec – May', 'Elec – Apr', 'Elec – March', 'Elec – Feb', 'Elec – Jan'])
metrictopull.extend(['NG – Dec', 'NG – Nov', 'NG – Oct', 'NG – Sept', 'NG – Aug', 'NG – July', 'NG – June', 'NG – May', 'NG – Apr', 'NG – March', 'NG – Feb', 'NG – Jan'])

with open(output_csvfile,'w',newline='') as output:
	i = len(metrictopull)
	writer = csv.writer(output,  delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	writer.writerow(metrictopull[0:i])

del metrictopull[0:27]
target_ibdex = metrictopull.index('Elec – Dec')
metrictopull = metrictopull[:target_ibdex]


i = 0
while i < len(propertyIDs):
	b = 0
	metricpulled = []
	while b < NumberofRows_metricsPM:
		t = metrictopull[b:9+b]
		y = ', '.join(t)
		# UPDATE METRICS TO MATCH REPORTING TEMPLATE
		# ADD SEED UPLOAD
		# ADD SEED REPORTING TEMPLATE PULL

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
	roottemp3 = ES_Client.get_property_identifiers_list(propertyIDs[i])
	root3 = Et.fromstring(roottemp3.encode('utf-8'))
	LEEDID_Report = ''
	CoStar_Report = ''
	customID_Report = ''
	DCRealBuildingID_Report = ''
	DCRealPropertyID_Report = ''
	for child4 in root3.findall('additionalIdentifier'):
		for child5 in child4.findall('additionalIdentifierType'):
			if child5.get('name') == 'District of Columbia Real Property Unique ID':
				DCRealPropertyID_Report = child4.find('value').text
				if DCRealPropertyID_Report is None:
					DCRealPropertyID_Report = ' '
			if child5.get('name') == 'District of Columbia Building Unique ID':
				DCRealBuildingID_Report = child4.find('value').text
				if DCRealBuildingID_Report is None:
					DCRealBuildingID_Report = ' '
			if child5.get('name') == 'Custom Property ID 1 - ID':
				customID_Report = child4.find('value').text
				if customID_Report is None:
					customID_Report = ' '
			if child5.get('name') == 'CoStar Property ID':
				CoStarID_Report = child4.find('value').text
				if CoStarID_Report is None:
					CoStar_Report = ' '
			if child5.get('name') == 'LEED US Project ID':
				LEEDID_Report = child4.find('value').text
				if LEEDID_Report is None:
					LEEDID_Report = ' '
	roottemp4 = ES_Client.get_meter_association(propertyIDs[i])
	root4 = Et.fromstring(roottemp4)
	meteredEnergyArea = ''
	meteredWaterArea = ''
	for child6 in root4.findall('energyMeterAssociation'):
		for child7 in child6.findall('propertyRepresentation'):
			meteredEnergyArea = child7.find('propertyRepresentationType').text
		if meteredEnergyArea is None:
			meteredEnergyArea = ''
	for child8 in root4.findall('waterMeterAssociation'):
		for child9 in child8.findall('propertyRepresentation'):
			meteredWaterArea = child9.find('propertyRepresentationType').text
		if meteredWaterArea is None:
			meteredWaterArea = ''

	metricpulled.insert(0,meteredEnergyArea)
	metricpulled.insert(0,meteredWaterArea)
	metricpulled.insert(0,LEEDID_Report)
	metricpulled.insert(0,CoStar_Report)
	metricpulled.insert(0,customID_Report)
	metricpulled.insert(0,DCRealBuildingID_Report)
	metricpulled.insert(0,DCRealPropertyID_Report)
	metricpulled.insert(0,tstamp2)
	metricpulled.insert(0,propertyContactName_Custom[i])
	metricpulled.insert(0,propertyContactEmail_Custom[i])
	metricpulled.insert(0,property_Notes[i])
	metricpulled.insert(0,irrigated_Area[i])
	metricpulled.insert(0,occupancy[i])
	metricpulled.insert(0,year_Built[i])
	metricpulled.insert(0,numberOfBuildings[i])
	metricpulled.insert(0,construction_Status[i])
	metricpulled.insert(0,primaryPropertyType_SelfSelected[i])
	metricpulled.insert(0,country[i])
	metricpulled.insert(0,postal_Code[i])
	metricpulled.insert(0,state_Province[i])
	metricpulled.insert(0,city[i])
	metricpulled.insert(0,address2[i])
	metricpulled.insert(0,address1[i])
	metricpulled.insert(0,propertyName[i])
	metricpulled.insert(0,DCRealPropertyIDs_CustomAdditional[i])
	metricpulled.insert(0,DCRealPropertyIDs_Custom[i])
	metricpulled.insert(0,propertyIDs[i])
	
	with open(output_csvfile,'a',newline='') as output:
		writer = csv.writer(output,  delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
		writer.writerow(metricpulled)
	i += 1



## SEED Upload

with open(output_csvfile, 'rb') as f:
	payload = {'file': (output_csvfile,f)}
	SEED_Upload_Data = SEED_Client.post_newfile(SEEDimportRecord, payload)
	SEED_Upload_Data_temp = json.loads(SEED_Upload_Data.text)
	SEED_Upload_Data_ID = str(SEED_Upload_Data_temp['import_file_id'])
	payload2 = {'cycle_id': SEEDcycleID, 'organization_id': SEEDorgID}
	data_save = SEED_Client.post_save_newfile(SEED_Upload_Data_ID, payload2)
	

payloadtest = {'username': 'District_of_Columbia_Annual_Reporting', 'password': 'DCsunshineESPM_WebServices!'}

SEED_pm_list = SEED_Client_v2_1.post_portfolio_manager_list(payloadtest)
SEED_pm_list_values = json.loads(SEED_pm_list.text)

i = 0 
b = 0
while i != 1:
	if str(SEED_pm_list_values["templates"][b]["id"]) == str(SEEDtoPMreportID):
		SEED_pm_list_values_template = SEED_pm_list_values["templates"][b]
		i += 1
	b += 1

payload3 = {'username': 'District_of_Columbia_Annual_Reporting', 'password': 'DCsunshineESPM_WebServices!','template': SEED_pm_list_values_template}

SEED_Upload_pm = SEED_Client_v2_1.post_portfolio_manager_import(payload3)
SEED_Upload_pm_temp = json.loads(SEED_Upload_pm.text)
SEED_Upload_pm = SEED_Upload_pm_temp['properties']
print (SEED_Upload_pm)

payload10 = {'import_record_id': SEEDimportRecord, 'properties': SEED_Upload_pm}
print (payload10)
SEED_Upload_pm_save = SEED_Client.post_portolio_manager_save_data(payload10)
SEED_Upload_pm_save_temp = json.loads(SEED_Upload_pm_save.text)
SEED_Upload_pm_save_ID = str(SEED_Upload_pm_save_temp['import_file_id'])

payload5 = {'cycle_id': SEEDcycleID, 'organization_id': SEEDorgID}
pm_data_save = SEED_Client.post_save_newfile(SEED_Upload_pm_save_ID, payload5)
print (pm_data_save.text)


