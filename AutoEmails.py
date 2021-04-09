#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  AutoEmails.py
#  
#  Copyright 2019 Andrew.Held <Andrew.Held@DOEE-1MQPXK2>
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  
#  

import sys
import xml.etree.ElementTree as Et
from SEEDAPI import SEEDClient 
from ast import literal_eval
import numpy as np
import pandas as pd
from datetime import timedelta
from email.mime.text import MIMEText
from O365 import MSGraphProtocol, Connection, Account
from O365.message import  Message
from validate_email import validate_email
from Data_Quality_Checks_singleyear import Data_quality_checker
import requests, json, logging, time, smtplib, os, csv, xlrd, array, datetime

def main(args):
    return 0

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)
"""
def no_score_text(args,PMID):
	resource = "https://portfoliomanager.energystar.gov/ws/property/("+str(PMID)+")/reasonsForNoScore?year=(2019)&month=(12)"
	response = requests.get(resource, auth=(PMUserName, PMPassword))
	print (response.text)
	
	return text_for_email
"""

if __name__ == '__main__':

	
	createFolder('./Output/')
	ts =  time.gmtime()
	tstamp =  (time.strftime("%Y%m%d_%H%M%S", ts))
	tstamp2 = (time.strftime("%m/%d/%y %H:%M:%S %p", ts))
	liveEnvironmentAccounts = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='liveEnvironmentAccounts', dtype = str)
	emailBodys  = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='emailBodys', dtype = str) 
	DQCemails = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='dataQualityEmails', dtype = str)
	pmMetricsToPull = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='pmMetricsToPull', dtype = str) 
	PMUserName = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='ENERGY STAR Account','Account UserName'].values[0])
	PMPassword = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='ENERGY STAR Account','Account Password'].values[0])
	o365UserName = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='Office365','Account UserName'].values[0])
	o365Password = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='Office365','Account Password'].values[0])
	SEEDUserName = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='SEED','Account UserName'].values[0])
	SEEDPassword = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='SEED','Account Password'].values[0])
	SEEDorgID = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='SEED','Org Info'].values[0])
	SEEDcycleID = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='SEED','Cycle ID'].values[0])
	SEEDcycleYears = [x for x in pmMetricsToPull['yeartoPull'] if str(x) !='nan']
	SEEDcycleYear = 'CY '+SEEDcycleYears[0]
	print (SEEDcycleYear)


	credentials = (o365UserName, o365Password)
	scopes = ["https://graph.microsoft.com/Mail.ReadWrite", "https://graph.microsoft.com/Mail.Send"]
	account = Account(credentials = credentials, scopes = scopes)	
	account.connection.refresh_token
	

	
	
	#SEED Log-in
	SEEDClient = SEEDClient(SEEDUserName, SEEDPassword, SEEDorgID)
	SEEDcycleList = json.loads(SEEDClient.get_cycles().text)
	SEEDcycleList = SEEDcycleList['cycles']

	
	for item in SEEDcycleList:
		if item['name'] == SEEDcycleYear:
			SEEDcycleID = str(item['id'])
	### Pulls data from Data_Quality_Checks

	data_checker = Data_quality_checker(SEEDUserName = SEEDUserName, SEEDPassword = SEEDPassword, SEEDorgID = SEEDorgID, SEEDcycleID = SEEDcycleID, SEEDcycleYear = SEEDcycleYear)
	SEED_data = data_checker.quality_check()
	
	


	### Filters based on Compliant, Exempt



	#SEED_data = SEED_data.drop(SEED_data[(SEED_data['Compliant'] == 'Applied') & (SEED_data['Resubmission Recieved'] != 'Applied')].index)
	## Took this off because can't figure out what we need to do about buildings that are compliant with issues we ignored.
	SEED_data = SEED_data.drop(SEED_data[SEED_data['Compliant'] == 'Applied'].index)
	SEED_data = SEED_data.drop(SEED_data[SEED_data['Email Sent'] == 'Applied'].index)
	SEED_data = SEED_data.drop(SEED_data[SEED_data['Exempted – Permanent'] == 'Applied'].index)
	SEED_data = SEED_data.drop(SEED_data[SEED_data['Exempted – This Cycle'] == 'Applied'].index)
	SEED_data = SEED_data.drop(SEED_data[SEED_data['PUBLIC BUILDINGS'] == 'Applied'].index)	
	SEED_data = SEED_data.drop(SEED_data[SEED_data['Resubmission Required'] == 'Applied'].index)	
	SEED_data = SEED_data.drop(SEED_data[SEED_data['Duplicate Report - Not for Disclosure'] == 'Applied'].index)	
	SEED_data = SEED_data.drop(SEED_data[SEED_data['Response Recieved'] == 'Applied'].index)	
	SEED_data = SEED_data.drop(SEED_data[SEED_data['Email Unable to Send'] == 'Applied'].index)
	SEED_data.to_excel('Output\\Reports_to_email'+tstamp+'.xlsx')	

	### Segment Email Type
	Issues_filter = []
	for col in DQCemails['Data Quality Flag']:
		condition = (SEED_data[col] == 'Issue')
		Issues_filter.append(condition)
		
	No_Issues_filter = []
	for col in DQCemails['Data Quality Flag']:
		condition = (SEED_data[col] == 'No Issue')
		No_Issues_filter.append(condition)

	Issues_filter_all = np.array(Issues_filter).any(axis=0)
	No_Issues_filter_all = np.array(No_Issues_filter).all(axis=0)
	SEED_data['Property Data Administrator - Email'] = SEED_data['Property Data Administrator - Email'].fillna('Missing')
	SEED_data = SEED_data.reset_index()
	SEED_data_issues = SEED_data.loc[Issues_filter_all]
	SEED_data_issues_resub = SEED_data_issues.loc[SEED_data_issues['Resubmission Recieved'] == 'Applied']
	SEED_data_issues_noresub= SEED_data_issues.loc[SEED_data_issues['Resubmission Recieved'] != 'Applied']
	SEED_data_noissues = SEED_data.loc[No_Issues_filter_all]
	SEED_data_noissues_resub = SEED_data_noissues.loc[SEED_data_noissues['Resubmission Recieved'] == 'Applied']
	SEED_data_noissues_noresub= SEED_data_noissues.loc[SEED_data_noissues['Resubmission Recieved'] != 'Applied']
	#IF YOU WANT TO LIMIT THE NUMBER OF EMAILS THAT GO OUT
	#SEED_data_issues_noresub = SEED_data_issues_noresub.head(500)
	
	
	



	### Format and Sends email
	"""
	for index, row in SEED_data_issues_resub.iterrows():
		is_valid = validate_email(row['Property Data Administrator - Email'], verify = True)
		if is_valid != False:
			messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Questions Resubmission','Subject'].values[0]
			messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Questions Resubmission','Body'].values[0]
			#messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Questions Resubmission Pre April 1st','Subject'].values[0]
			#messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Questions Resubmission Pre April 1st','Body'].values[0]
			alert_body_text = []
			instruction_body_text = []
			for index, check in DQCemails.iterrows():
				print(row['PM Property ID'])
				
				if row[check['Data Quality Flag']] == "Issue":
					alert_body_text.append('<li>'+check['Alert Text']+'</li>')
					instruction_body_text.append('<li>'+check['Instruction Text']+'</li>')
				else:
					pass
					
					
			messageSubject = messageSubject.replace('[REPORTEDYEAR]',SEEDcycleYear.replace('CY ',''))
			messageSubject = messageSubject.replace('[PROPERTYNAME]',row['Property Name'])			
			messageSubject = messageSubject.replace('[PMID]',row['PM Property ID'])	
			messageSubject = messageSubject.replace('[DCID]',str(row['District Of Columbia Real Property Unique ID']))
			messageBody = messageBody.replace('[CONTACT NAME]', row['Property Data Administrator'])
			messageBody = messageBody.replace('[ALERT BODY FULL]', ''.join(alert_body_text))
			messageBody = messageBody.replace('[INSTRUCTION BODY FULL]', ''.join(instruction_body_text))
			messageBody = messageBody.replace('[REPORTEDYEAR]',SEEDcycleYear.replace('CY ',''))		
			messageBody = messageBody.replace('[PMID]',row['PM Property ID'])	
			messageBody = messageBody.replace('[DCID]',str(row['District Of Columbia Real Property Unique ID']))			
			messageBody = messageBody.replace('[PROPERTY ADDRESS]',row['Address line 1 - custom'])		
			messageBody = messageBody.replace('[PROPERTY NAME]',row['Property Name'])	
			messageBody = messageBody.replace('[EUIPERCENT]',str(row['% Difference From National Median Source EUI']))
			messageBody = messageBody.replace('[GFA]',str(row['Property GFA - Calculated (Buildings) (ft2)']))
			messageBody = messageBody.replace('[OTRGFA]',str(row['SumOfGBA (Tax Lot)']))
			messageBody = messageBody.replace('[EUI]',str(row['Source EUI (kBtu/ft²/year)']))
			messageBody = messageBody.replace('[NEXTDATAYEAR]',str(int(SEEDcycleYear.replace('CY ',''))+1))	
			try:
				messageBody = messageBody.replace('[ELECPERCENT]',(str(round(100*(float(row['Electricity Use - Grid Purchase And Generated From Onsite Renewable Systems (kWh)'])*3.412)/ (float(row['Site Energy Use (kBtu)'])),2))))
			except:
				messageBody = messageBody.replace('[ELECPERCENT]','0')
			messageBody = messageBody.replace('[ENERGYSTARScore]',str(row['ENERGY STAR Score']))
			messageBody = messageBody.replace('[DUEDATE]','April 15th, 2021')																	
			m = account.new_message()
			m.sender.address = 'info.benchmark@DC.gov'
			#m.to.add('andrew.held@dc.gov')
			m.to.add(row['Property Data Administrator - Email'])
			m.to.add(row['Owner Email'])
			m.cc.add('info.benchmark@DC.gov')	
			m.subject = messageSubject
			m.body = messageBody
			pass
			try:
				m.send()
			
				note = 'sent to:' + row['Property Data Administrator - Email'] + ' ' + row['Owner Email'] + m.body
				payload = {'name':'Automatically Created','note_type':'Note','organization_id':SEEDorgID,'text':note}
				#SEEDClient.post_property_note(row['property_view_id'],payload)
				payload2 = {'inventory_ids':[row['property_view_id']],'add_label_ids':[4687],'remove_label_ids':[5079, 4672]}
				SEEDClient.put_add_label(payload2)
			except:
				
				
				note = 'email not sent, bad email address'
				payload = {'name':'Automatically Created','note_type':'Note','organization_id':SEEDorgID,'text':note}
				#SEEDClient.post_property_note(row['property_view_id'],payload)		
				payload2 = {'inventory_ids':[row['property_view_id']],'add_label_ids':[5520],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload2)		
		else:
			pass
	"""
	for index, row in SEED_data_issues_noresub.iterrows():
		is_valid = validate_email(row['Property Data Administrator - Email'], verify = True)
		if is_valid != False:
			messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Questions','Subject'].values[0]
			messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Questions','Body'].values[0]
			#messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Questions Pre April 1st','Subject'].values[0]
			#messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Questions Pre April 1st','Body'].values[0]
			alert_body_text = []
			instruction_body_text = []
			for index, check in DQCemails.iterrows():
				if row[check['Data Quality Flag']] == "Issue":
					alert_body_text.append('<li>'+check['Alert Text']+'</li>')
					instruction_body_text.append('<li>'+check['Instruction Text']+'</li>')
				else:
					pass
					
					
			messageSubject = messageSubject.replace('[DATAYEAR]',SEEDcycleYear.replace('CY ',''))
			messageSubject = messageSubject.replace('[PROPERTYNAME]',row['Property Name'])			
			messageSubject = messageSubject.replace('[PMID]',row['PM Property ID'])	
			messageSubject = messageSubject.replace('[DCIDS]',str(row['District Of Columbia Real Property Unique ID']))
			messageBody = messageBody.replace('[CONTACTNAME]', row['Property Data Administrator'])
			messageBody = messageBody.replace('[ALERT BODY FULL]', ''.join(alert_body_text))
			messageBody = messageBody.replace('[INSTRUCTION BODY FULL]', ''.join(instruction_body_text))
			messageBody = messageBody.replace('[DATAYEAR]',SEEDcycleYear.replace('CY ',''))		
			messageBody = messageBody.replace('[PMID]',row['PM Property ID'])	
			messageBody = messageBody.replace('[DCIDS]',str(row['District Of Columbia Real Property Unique ID']))			
			messageBody = messageBody.replace('[PROPERTYADDRESS]',row['Address line 1 - custom'])		
			messageBody = messageBody.replace('[PROPERTYNAME]',row['Property Name'])	
			messageBody = messageBody.replace('[EUIPERCENT]',str(row['% Difference From National Median Source EUI']))
			messageBody = messageBody.replace('[GFA]',str(row['Property GFA - Calculated (Buildings) (ft2)']))
			messageBody = messageBody.replace('[OTRGFA]',str(row['SumOfGBA (Tax Lot)']))
			messageBody = messageBody.replace('[EUI]',str(row['Source EUI (kBtu/ft²/year)']))
			messageBody = messageBody.replace('[NEXTDATAYEAR]',str(int(SEEDcycleYear.replace('CY ',''))+1))	
			try:
				messageBody = messageBody.replace('[ELECPERCENT]',(str(round(100*(float(row['Electricity Use - Grid Purchase And Generated From Onsite Renewable Systems (kWh)'])*3.412)/ (float(row['Site Energy Use (kBtu)'])),2))))
			except:
				messageBody = messageBody.replace('[ELECPERCENT]','0')
			messageBody = messageBody.replace('[ENERGYSTARScore]',str(row['ENERGY STAR Score']))
			messageBody = messageBody.replace('[DUEDATE]','April 16th, 2021')																	
																
			m = account.new_message()
			m.sender.address = 'info.benchmark@DC.gov'
			#m.to.add('andrew.held@dc.gov')
			m.to.add(row['Property Data Administrator - Email'])
			m.to.add(row['Owner Email'])
			m.cc.add('info.benchmark@DC.gov')	
			m.subject = messageSubject
			m.body = messageBody
			print (m)
			pass
			try:
				m.send()
			
				note = 'sent to:' + row['Property Data Administrator - Email'] + ' ' + row['Owner Email'] + m.body
				payload = {'name':'Automatically Created','note_type':'Note','organization_id':SEEDorgID,'text':note}
				#SEEDClient.post_property_note(row['property_view_id'],payload)
				payload2 = {'inventory_ids':[row['property_view_id']],'add_label_ids':[4687],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload2)
			except:
				note = 'email not sent, bad email address'
				payload = {'name':'Automatically Created','note_type':'Note','organization_id':SEEDorgID,'text':note}
				#SEEDClient.post_property_note(row['property_view_id'],payload)
				payload2 = {'inventory_ids':[row['property_view_id']],'add_label_ids':[5520],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload2)							

		else:
			pass			





	SEED_Property_Column_Name_List = json.loads(SEEDClient.get_property_column_names().text)
	SEED_Property_Column_Name_List = SEED_Property_Column_Name_List['columns']
	Property_Column_Name_Lookup_temp1 = []
	Property_Column_Name_Lookup_temp2 = []
	for item in SEED_Property_Column_Name_List:
		Property_Column_Name_Lookup_temp1.append(item['name'])
		Property_Column_Name_Lookup_temp2.append(item['display_name'])
	Property_Column_Name_Lookup = dict(zip(Property_Column_Name_Lookup_temp1,Property_Column_Name_Lookup_temp2))
	
	data_View = json.loads(SEEDClient.post_property_list_complete_view(SEEDcycleID).text)
	data_View = pd.io.json.json_normalize(data_View['results'])
	data_View.drop(columns=['related'], inplace = True)

 
	for col in data_View.columns:
		if col in Property_Column_Name_Lookup.keys():
			data_View.rename(columns={col:Property_Column_Name_Lookup.get(col)},inplace = True)
			


	data_View.drop(columns=['id','notes_count','property_state_id','property_view_id','merged_indicator','bounding_box','long_lat','centroid'], inplace = True)
	data_View = data_View[['Property Name','Parent Property Name','Address line 1 - custom','Address 2','City','State/Province','Postal Code','Property GFA - Self-Reported (ft2)','Property GFA - Calculated (Buildings And Parking) (ft2)','Property GFA - Calculated (Buildings) (ft2)','Property GFA - Calculated (Parking) (ft2)','Primary Property Type - Self Selected','Primary Property Type - Portfolio Manager-Calculated','National Median Reference Property Type','List Of All Property Use Types At Property','Largest Property Use Type','Largest Property Use Type - Gross Floor Area (ft2)','2nd Largest Property Use Type','2nd Largest Property Use - Gross Floor Area (ft2)','3rd Largest Property Use Type','3rd Largest Property Use Type - Gross Floor Area (ft2)','Irrigated Area (ft2)','Construction Status','Building Count','Year Built','Occupancy','Property Notes','Third Party Certification','Last Modified Date - Property','Last Modified By - Property','Last Modified Date - Property Use','Last Modified By - Property Use','Last Modified Date - Property Use Details','Last Modified By - Property Use Details','Last Modified Date - Electric Meters','Last Modified By - Electric Meters','Last Modified Date - Gas Meters','Last Modified By - Gas Meters','Last Modified Date - Non-Electric Non-Gas Energy Meters','Last Modified By - Non-Electric Non-Gas Energy Meters','Last Modified Date - Water Meters','Last Modified By - Water Meters','Property Data Administrator','Property Data Administrator - Email','On Behalf Of','Owner Email','Owner Telephone','Service And Product Provider','Metered Areas (Energy)','Metered Areas (Water)','Cooling Degree Days (CDD) (°F)','Heating Degree Days (HDD) (°F)','PM Property ID','PM Parent Property ID','Custom Property ID 1 - ID','District Of Columbia Building Unique ID','District Of Columbia Real Property Unique ID','District Of Columbia Real Property Unique ID - Web Services','District Of Columbia Real Property Unique ID - Additional Web Services','LEED US Project ID','ENERGY STAR Certification - Year(s) Certified','ENERGY STAR Certification - Last Approval Date','ENERGY STAR Certification - Eligibility','Energy Current Date','ENERGY STAR Score','National Median Source Energy Use (kBtu)','National Median Site Energy Use (kBtu)','National Median Site EUI','National Median Source EUI','% Difference From National Median Site EUI','% Difference From National Median Source EUI','Site Energy Use (kBtu)','Source Energy Use (kBtu)','Site EUI (kBtu/ft²/year)','Source EUI (kBtu/ft²/year)','Weather Normalized Site Energy Use (kBtu)','Weather Normalized Source Energy Use (kBtu)','Site EUI Weather Normalized (kBtu/ft²/year)','Source EUI Weather Normalized (kBtu/ft²/year)','Weather Normalized Site Electricity (kWh)','Weather Normalized Site Natural Gas Use (therms)','Site Energy Use - Adjusted to Current Year (kBtu)','Source Energy Use - Adjusted to Current Year (kBtu)','Site EUI - Adjusted to Current Year (kBtu/ft2)','Source EUI - Adjusted to Current Year (kBtu/ft2)','Water/Wastewater Site EUI - Adjusted to Current Year (kBtu/gpd)','Water/Wastewater Source EUI - Adjusted to Current Year (kBtu/gpd)','Water Current Date','Water Score (Multifamily Only)','Water Use Intensity (All Water Sources) (gal/ft2)','Water Use (All Water Sources) (kgal)','Indoor Water Use (All Water Sources) (kgal)','Indoor Water Use Intensity (All Water Sources) (gal/ft2)','Outdoor Water Use (All Water Sources) (kgal)','Municipally Supplied Potable Water - Mixed Indoor/Outdoor Use (kgal)','Total Waste (Disposed And Diverted) (Tons)','Diversion Rate (%)','Total GHG Emissions (Metric Tons CO2e)','Total GHG Emissions Intensity (kgCO2e/ft2)','Direct GHG Emissions Intensity (kgCO2e/ft2)','Indirect GHG Emissions (Metric Tons CO2e)','Indirect GHG Emissions Intensity (kgCO2e/ft2)','eGRID Output Emissions Rate (kgCO2e/MBtu)','National Median Total GHG Emissions (Metric Tons CO2e)','Period Ending Date','Electricity Use - Grid Purchase And Generated From Onsite Renewable Systems (kWh)','Electricity Use - Grid Purchase (kWh)','Electricity Use - Generated From Onsite Renewable Systems And Used Onsite (kWh)','Natural Gas Use (therms)','Fuel Oil #1 Use (kBtu)','Fuel Oil #2 Use (kBtu)','Diesel #2 Use (kBtu)','District Steam Use (kBtu)','District Hot Water Use (kBtu)','District Chilled Water Use (kBtu)','Elec - Jan','Elec - Feb','Elec - March','Elec - Apr','Elec - May','Elec - June','Elec - July','Elec - Aug','Elec - Sept','Elec - Oct','Elec - Nov','Elec - Dec','NG - Jan','NG - Feb','NG - March','NG - Apr','NG - May','NG - June','NG - July','NG - Aug','NG - Sept','NG - Oct','NG - Nov','NG - Dec','Percent Of Total Electricity Generated From Onsite Renewable Systems','Green Power - Onsite (kWh)','Green Power - Offsite (kWh)','Percent Of Electricity That Is Green Power','Avoided Emissions - Onsite Green Power (Metric Tons CO2e)','Avoided Emissions - Offsite Green Power (Metric Tons CO2e)','Net Emissions (Metric Tons CO2e)','Data Center - UPS Output Meter (kWh)','Data Center - PDU Input Meter (kWh)','Data Center - PDU Output Meter (kWh)','Data Center - IT Equipment Input Meter (kWh)','Data Center - IT Site Energy (kWh)','Data Center - IT Source Energy (kBtu)','Data Center - PUE','Data Center - National Median PUE','Adult Education - Gross Floor Area (ft2)','Ambulatory Surgical Center - Gross Floor Area (ft2)','Aquarium - Gross Floor Area (ft2)','Automobile Dealership - Gross Floor Area (ft2)','Bank Branch - Computer Density (Number per 1,000 sq ft)','Bank Branch - Gross Floor Area (ft2)','Bank Branch - Number of Computers','Bank Branch - Number of Workers on Main Shift','Bank Branch - Percent That Can Be Cooled','Bank Branch - Percent That Can Be Heated','Bank Branch - Weekly Operating Hours','Bank Branch - Worker Density (Number per 1,000 sq ft)','Bar/Nightclub - Gross Floor Area (ft2)','Barracks- Gross Floor Area (ft2)','Barracks- Number of Rooms','Barracks- Percent That Can Be Cooled','Barracks- Percent That Can Be Heated','Barracks - Room Density (Number per 1,000 sq ft)','Bowling Alley - Gross Floor Area (ft2)','Casino - Gross Floor Area (ft2)','College/University - Gross Floor Area (ft2)','Convenience Store with Gas Station - Gross Floor Area (ft2)','Convenience Store Without Gas Station - Gross Floor Area (ft2)','Convention Center - Gross Floor Area (ft2)','Courthouse - Computer Density (Number per 1,000 sq ft)','Courthouse - Gross Floor Area (ft2)','Courthouse - Number of Computers','Courthouse - Number of Workers on Main Shift','Courthouse - Percent That Can Be Cooled','Courthouse - Percent That Can Be Heated','Courthouse - Weekly Operating Hours','Courthouse - Worker Density (Number per 1,000 sq ft)','Data Center - Cooling Equipment Redundancy','Data Center - Energy Estimates Applied','Data Center - Gross Floor Area (ft2)','Data Center - IT Energy Configuration','Data Center - UPS System Redundancy','Distribution Center - Clear Height (ft)','Distribution Center - Gross Floor Area (ft2)','Distribution Center - Number of Walk-in Refrigeration/Freezer Units','Distribution Center - Number of Workers on Main Shift','Distribution Center - Percent That Can Be Cooled','Distribution Center - Percent That Can Be Heated','Distribution Center - Percent Used for Cold Storage','Distribution Center - Walk-in Refrigeration Density (Number per 1,000 sq ft)','Distribution Center - Weekly Operating Hours','Distribution Center - Worker Density (Number per 1,000 sq ft)','Drinking Water Treatment & Distribution - Average Flow (MGD)','Drinking Water Treatment & Distribution - Gross Floor Area (ft2)','Enclosed Mall - Gross Floor Area (ft2)','Energy/Power Station - Gross Floor Area (ft2)','Fast Food Restaurant - Gross Floor Area (ft2)','Financial Office - Computer Density (Number Per 1,000 Ft2)','Financial Office - Gross Floor Area (ft2)','Financial Office - Number Of Computers','Financial Office - Number Of Workers On Main Shift','Financial Office - Percent That Can Be Cooled','Financial Office - Percent That Can Be Heated','Financial Office - Weekly Operating Hours','Financial Office - Worker Density (Number Per 1,000 Ft2)','Fire Station - Gross Floor Area (ft2)','Fitness Center/Health Club/Gym - Gross Floor Area (ft2)','Food Sales - Gross Floor Area (ft2)','Food Service - Gross Floor Area (ft2)','Swimming Pool - Approximate Pool Size','Swimming Pool - Location Of Pool','Swimming Pool - Months In Use','Hospital (General Medical & Surgical)- Full Time Equivalent (FTE) Workers','Hospital (General Medical & Surgical) - Full Time Equivalent (FTE) Workers Density (Number per 1,000 sq ft)','Hospital (General Medical & Surgical) - Gross Floor Area (ft2)','Hospital (General Medical & Surgical) - Gross Floor Area Used for Food Preparation (ft2)','Hospital (General Medical & Surgical) - Laboratory','Hospital (General Medical & Surgical) - Licensed Bed Capacity','Hospital (General Medical & Surgical) - Licensed Bed Capacity Density (Number per 1,000 sq ft)','Hospital (General Medical & Surgical) - Maximum Number of Floors','Hospital (General Medical & Surgical) - MRI Density (Number per 1,000 sq ft)','Hospital (General Medical & Surgical) - Number of MRI Machines','Hospital (General Medical & Surgical) - Number of Staffed Beds','Hospital (General Medical & Surgical) - Number of Sterilization Units','Hospital (General Medical & Surgical) - Number of Workers on Main Shift','Hospital (General Medical & Surgical) - Number of Workers on Main Shift Density (Number per 1,000 sq ft)','Hospital (General Medical & Surgical) - Onsite Laundry Facility','Hospital (General Medical & Surgical) - Owned By','Hospital (General Medical & Surgical) - Percent That Can Be Cooled','Hospital (General Medical & Surgical) - Percent That Can Be Heated','Hospital (General Medical & Surgical) - Staffed Bed Density (Number per 1,000 sq ft)','Hospital (General Medical & Surgical) - Sterilization Density (Number per 1,000 sq ft)','Hospital (General Medical & Surgical) - Tertiary Care','Hotel - Amount Of Laundry Processed On-site Annually (short Tons/year)','Hotel - Commercial Refrigeration Density (Number Per 1,000 Ft2)','Hotel - Cooking Facilities','Hotel - Full Service Spa Floor Area (ft2)','Hotel - Gross Floor Area (ft2)','Hotel - Gym/fitness Center Floor Area (ft2)','Hotel - Hours Per Day Guests Onsite','Hotel - Number Of Commercial Refrigeration/Freezer Units','Hotel - Number Of Guest Meals Served Per Year','Hotel - Number Of Rooms','Hotel - Number Of Workers On Main Shift','Hotel - Percent That Can Be Cooled','Hotel - Percent That Can Be Heated','Hotel - Room Density (Number Per 1,000 Ft2)','Hotel - Type Of Laundry Facility','Hotel - Worker Density (Number Per 1,000 Ft2)','Ice/Curling Rink - Gross Floor Area (ft2)','Indoor Arena - Gross Floor Area (ft2)','K-12 School - Computer Density (Number Per 1,000 Ft2)','K-12 School - Cooking Facilities','K-12 School - Gross Floor Area (ft2)','K-12 School - Gymnasium Floor Area (ft2)','K-12 School - High School','K-12 School - Months In Use','K-12 School - Number Of Computers','K-12 School - Number Of Walk-in Refrigeration/Freezer Units','K-12 School - Number Of Workers On Main Shift','K-12 School - Percent That Can Be Cooled','K-12 School - Percent That Can Be Heated','K-12 School - Refrigeration Density (Number Per 1,000 Ft2)','K-12 School - School District','K-12 School - Student Seating Capacity','K-12 School - Student Seating Density (Number Per 1,000 Ft2)','K-12 School - Weekend Operation','K-12 School - Worker Density (Number Per 1,000 Ft2)','Laboratory - Gross Floor Area (ft2)','Library - Gross Floor Area (ft2)','Lifestyle Center - Gross Floor Area (ft2)','Mailing Center/Post Office - Gross Floor Area (ft2)','Manufacturing/Industrial Plant - Gross Floor Area (ft2)','Medical Office - Computer Density (Number per 1,000 sq ft)','Medical Office - Gross Floor Area (ft2)','Medical Office - MRI Machine Density (Number per 1,000 sq ft)','Medical Office - Number of Computers','Medical Office - Number of MRI Machines','Medical Office - Number of Surgical Operating Beds','Medical Office - Number of Workers on Main Shift','Medical Office - Percent That Can Be Cooled','Medical Office - Percent That Can Be Heated','Medical Office - Surgery Center Floor Area (ft2)','Medical Office - Surgical Operating Bed Density (Number per 1,000 sq ft)','Medical Office - Weekly Operating Hours','Medical Office - Worker Density (Number per 1,000 sq ft)','Movie Theater - Gross Floor Area (ft2)','Multifamily Housing - Government Subsidized Housing','Multifamily Housing - Gross Floor Area (ft2)','Multifamily Housing - Number Of Bedrooms','Multifamily Housing - Number Of Bedrooms Density (Number Per 1,000 Ft2)','Multifamily Housing - Number Of Laundry Hookups In All Units','Multifamily Housing - Number Of Laundry Hookups In Common Area(s)','Multifamily Housing - Number Of Residential Living Units In A High-Rise Building (10 Or More Stories)','Multifamily Housing - Number Of Residential Living Units In A High-Rise Building Density (Number Per 1,000 Ft2)','Multifamily Housing - Number Of Residential Living Units In A Low-Rise Building (1-4 Stories)','Multifamily Housing - Number Of Residential Living Units In A Low-Rise Building Density (Number Per 1,000 Ft2)','Multifamily Housing - Number Of Residential Living Units In A Mid-Rise Building (5-9 Stories)','Multifamily Housing - Number Of Residential Living Units In A Mid-Rise Building Density (Number Per 1,000 Ft2)','Multifamily Housing - Percent That Can Be Cooled','Multifamily Housing - Percent That Can Be Heated','Multifamily Housing - Resident Population Type','Multifamily Housing - Total Number Of Residential Living Units','Multifamily Housing - Total Number Of Residential Living Units Density (Number Per 1,000 Ft2)','Museum - Gross Floor Area (ft2)','Non-Refrigerated Warehouse - Clear Height (ft)','Non-Refrigerated Warehouse - Gross Floor Area (ft2)','Non-Refrigerated Warehouse - Number of Walk-in Refrigeration/Freezer Units','Non-Refrigerated Warehouse - Number of Workers on Main Shift','Non-Refrigerated Warehouse - Percent That Can Be Cooled','Non-Refrigerated Warehouse - Percent That Can Be Heated','Non-Refrigerated Warehouse - Percent Used for Cold Storage','Non-Refrigerated Warehouse - Walk-in Refrigeration Density (Number per 1,000 sq ft)','Non-Refrigerated Warehouse - Weekly Operating Hours','Non-Refrigerated Warehouse - Worker Density (Number per 1,000 sq ft)','Office - Computer Density (Number Per 1,000 Ft2)','Office - Gross Floor Area (ft2)','Office - Number Of Computers','Office - Number Of Workers On Main Shift','Office - Percent That Can Be Cooled','Office - Percent That Can Be Heated','Office - Weekly Operating Hours','Office - Worker Density (Number Per 1,000 Ft2)','Other - Education - Gross Floor Area (ft2)','Other - Entertainment/Public Assembly - Gross Floor Area (ft2)','Other - Gross Floor Area (ft2)','Other - Lodging/Residential - Gross Floor Area (ft2)','Other - Mall - Gross Floor Area (ft2)','Other - Public Services - Gross Floor Area (ft2)','Other - Recreation - Gross Floor Area (ft2)','Other - Restaurant/Bar - Gross Floor Area (ft2)','Other - Services - Gross Floor Area (ft2)','Other - Specialty Hospital - Gross Floor Area (ft2)','Other - Stadium - Gross Floor Area (ft2)','Other - Technology/Science - Gross Floor Area (ft2)','Other - Utility - Gross Floor Area (ft2)','Outpatient Rehabilitation/Physical Therapy - Gross Floor Area (ft2)','Parking - Completely Enclosed Parking Garage Size (ft2)','Parking - Gross Floor Area (ft2)','Parking - Open Parking Lot Size (ft2)','Parking - Partially Enclosed Parking Garage Size (ft2)','Parking - Supplemental Heating','Performing Arts - Gross Floor Area (ft2)','Personal Services (Health/Beauty, Dry Cleaning, Etc.) - Gross Floor Area (ft2)','Police Station - Gross Floor Area (ft2)','Pre-school/Daycare - Gross Floor Area (ft2)','Prison/Incarceration - Gross Floor Area (ft2)','Race Track - Gross Floor Area (ft2)','Refrigerated Warehouse - Clear Height (ft)','Refrigerated Warehouse - Gross Floor Area (ft2)','Refrigerated Warehouse - Number of Workers on Main Shift','Refrigerated Warehouse - Percent That Can Be Cooled','Refrigerated Warehouse - Percent That Can Be Heated','Refrigerated Warehouse - Percent Used for Cold Storage','Refrigerated Warehouse - Weekly Operating Hours','Refrigerated Warehouse - Worker Density (Number per 1,000 sq ft)','Repair Services (Vehicle, Shoe, Locksmith, etc.) - Gross Floor Area (ft2)','Residence Hall/ Dormitory - Computer Lab','Residence Hall/ Dormitory - Dining Hall','Residence Hall/Dormitory - Gross Floor Area (ft2)','Residence Hall/Dormitory - Number of Rooms','Residence Hall/Dormitory - Percent That Can Be Cooled','Residence Hall/Dormitory - Percent That Can Be Heated','Residence Hall/Dormitory - Room Density (Number per 1,000 sq ft)','Residential Care Facility - Gross Floor Area (ft2)','Restaurant - Gross Floor Area (ft2)','Retail Store - Area of All Walk-in Refrigeration/Freezer Units (ft2)','Retail Store - Cash Register Density (Number per 1,000 sq ft)','Retail Store - Computer Density (Number per 1,000 sq ft)','Retail Store - Cooking Facilities','Retail Store - Exterior Entrance to the Public','Retail Store - Gross Floor Area (ft2)','Retail Store - Length of All Open or Closed Refrigeration/Freezer Units (ft)','Retail Store - Number of Cash Registers','Retail Store - Number of Computers','Retail Store - Number of Open or Closed Refrigeration/Freezer Units','Retail Store - Number of Walk-in Refrigeration/Freezer Units','Retail Store - Number of Workers on Main Shift','Retail Store - Open or Closed Refrigeration Density (Number per 1,000 sq ft)','Retail Store - Percent That Can Be Cooled','Retail Store - Percent That Can Be Heated','Retail Store - Single Store','Retail Store - Walk-in Refrigeration Density (Number per 1,000 sq ft)','Retail Store - Weekly Operating Hours','Retail Store - Worker Density (Number per 1,000 sq ft)','Roller Rink - Gross Floor Area (ft2)','Self-Storage Facility - Clear Height (ft)','Self-Storage Facility - Computer Density (Number per 1,000 sq ft)','Self-Storage Facility - Gross Floor Area (ft2)','Self-Storage Facility - Number of Computers','Self-Storage Facility - Number of Walk-in Refrigeration/Freezer Units','Self-Storage Facility - Number of Workers on Main Shift','Self-Storage Facility - Percent That Can Be Cooled','Self-Storage Facility - Percent That Can Be Heated','Self-Storage Facility - Percent Used For Cold Storage','Self-Storage Facility - Weekly Operating Hours','Self-Storage Facility - Worker Density (Number per 1,000 sq ft)','Senior Care Community - Average Number of Residents','Senior Care Community - Commercial Refrigeration Density (Number per 1,000 sq ft)','Senior Care Community - Commercial Washing Machine Density (Number per 1,000 sq ft)','Senior Care Community - Computer Density (Number per 1,000 sq ft)','Senior Care Community - Electronic Lift Density (Number per 1,000 sq ft)','Senior Care Community - Gross Floor Area (ft2)','Senior Care Community - Licensed Bed Capacity','Senior Care Community - Licensed Bed Capacity Density (Number per 1,000 sq ft)','Senior Care Community - Living Unit Density (Number per 1,000 sq ft)','Senior Care Community - Maximum Resident Capacity','Senior Care Community - Number of Commercial Refrigeration/ Freezer Units','Senior Care Community - Number of Commercial Washing Machines','Senior Care Community - Number of Computers','Senior Care Community - Number of Residential Electronic Lift Systems','Senior Care Community - Number of Residential Washing Machines','Senior Care Community - Number of Workers on Main Shift','Senior Care Community - Percent That Can Be Cooled','Senior Care Community - Percent That Can Be Heated','Senior Care Community - Resident Density (Number per 1,000 sq ft)','Senior Care Community - Residential Washing Machine Density (Number per 1,000 sq ft)','Senior Care Community - Total Number of Residential Living Units','Senior Care Community - Worker Density (Number per 1,000 sq ft)','Single Family Home - Gross Floor Area (ft2)','Social/Meeting Hall - Gross Floor Area (ft2)','Stadium (Closed) - Gross Floor Area (ft2)','Stadium (Open) - Gross Floor Area (ft2)','Strip Mall - Gross Floor Area (ft2)','Supermarket/Grocery - Area of All Walk-in Refrigeration/Freezer Units (ft2)','Supermarket/Grocery - Cash Register Density (Number per 1,000 sq ft)','Supermarket/Grocery - Computer Density (Number per 1,000 sq ft)','Supermarket/Grocery - Cooking Facilities','Supermarket/Grocery - Gross Floor Area (ft2)','Supermarket/Grocery - Length of All Open or Closed Refrigeration/Freezer Units (ft)','Supermarket/Grocery - Number of Cash Registers','Supermarket/Grocery - Number of Computers','Supermarket/Grocery - Number of Open or Closed Refrigeration/Freezer Units','Supermarket/Grocery - Number of Walk-in Refrigeration/Freezer Units','Supermarket/Grocery - Number of Workers on Main Shift','Supermarket/Grocery - Open or Closed Refrigeration Density (Number per 1,000 sq ft)','Supermarket/Grocery - Percent That Can Be Cooled','Supermarket/Grocery - Percent That Can Be Heated','Supermarket/Grocery - Walk-in Refrigeration Density (Number per 1,000 sq ft)','Supermarket/Grocery - Weekly Operating Hours','Supermarket/Grocery - Worker Density (Number per 1,000 sq ft)','Transportation Terminal/Station - Gross Floor Area (ft2)','Urgent Care/Clinic/Other Outpatient - Gross Floor Area (ft2)','Veterinary Office - Gross Floor Area (ft2)','Vocational School - Gross Floor Area (ft2)','Wastewater Treatment Plant - Average Effluent Biological Oxygen Demand (BOD5) (mg/l)','Wastewater Treatment Plant - Average Influent Biological Oxygen Demand (BOD5) (mg/l)','Wastewater Treatment Plant - Average Influent Flow (MGD)','Wastewater Treatment Plant - Fixed Film Trickle Filtration Process','Wastewater Treatment Plant - Gross Floor Area (ft2)','Wastewater Treatment Plant - Nutrient Removal','Wastewater Treatment Plant - Plant Design Flow Rate (MGD)','Wholesale Club/Supercenter- Cash Register Density (Number per 1,000 sq ft)','Wholesale Club/Supercenter- Computer Density (Number per 1,000 sq ft)','Wholesale Club/Supercenter - Cooking Facilities','Wholesale Club/Supercenter- Exterior Entrance to the Public','Wholesale Club/Supercenter- Gross Floor Area (ft2)','Wholesale Club/Supercenter - Length of All Open or Closed Refrigeration/Freezer Units (ft)','Wholesale Club/Supercenter- Number of Cash Registers','Wholesale Club/Supercenter- Number of Computers','Wholesale Club/Supercenter- Number of Open or Closed Refrigeration/Freezer Units','Wholesale Club/Supercenter- Number of Walk-in Refrigeration/Freezer Units','Wholesale Club/Supercenter- Number of Workers on Main Shift','Wholesale Club/Supercenter- Open or Closed Refrigeration Density (Number per 1,000 sq ft)','Wholesale Club/Supercenter- Percent That Can Be Cooled','Wholesale Club/Supercenter- Percent That Can Be Heated','Wholesale Club/Supercenter - Single Store','Wholesale Club/Supercenter- Walk-in Refrigeration Density (Number per 1,000 sq ft)','Wholesale Club/Supercenter- Weekly Operating Hours','Wholesale Club/Supercenter- Worker Density (Number per 1,000 sq ft)','Worship Facility - Commercial Refrigeration Density (Number per 1,000 sq ft)','Worship Facility - Computer Density (Number per 1,000 sq ft)','Worship Facility - Cooking Facilities','Worship Facility - Gross Floor Area (ft2)','Worship Facility - Gross Floor Area Used for Food Preparation (ft2)','Worship Facility - Number of Commercial Refrigeration/Freezer Units','Worship Facility - Number of Computers','Worship Facility - Number of Weekdays Open','Worship Facility - Open All Weekdays','Worship Facility - Percent That Can Be Cooled','Worship Facility - Percent That Can Be Heated','Worship Facility - Seating Capacity','Worship Facility - Weekly Operating Hours','Zoo - Gross Floor Area (ft2)','Estimated Values - Energy','Estimated Values - Water','Default Values','Temporary Values','Data Quality Checker Run','Data Quality Checker - Date Run','Alert - Gross Floor Area Is 0 Ft2','Alert - Property Has No Uses','Alert - Energy Meter Has Less Than 12 Full Calendar Months Of Data','Alert - Energy Meter Has Gaps','Alert - Energy Meter Has Overlaps','Alert - Energy - No Meters Selected For Metrics','Alert - Energy Meter Has Single Entry More Than 65 Days','Alert - Water Meter Has Less Than 12 Full Calendar Months Of Data','Alert - Water Meter Has Gaps','Alert - Water Meter Has Overlaps','Alert - Water - No Meters Selected For Metrics','Alert - Data Center Issue (with Estimates, IT Configuration, Or IT Meter)','Estimated Data Flag - Electricity (Grid Purchase)','Estimated Data Flag - Electricity (Onsite Solar)','Estimated Data Flag - Natural Gas','Estimated Data Flag - Fuel Oil (No. 1)','Estimated Data Flag - Fuel Oil (No. 2)','Estimated Data Flag - District Steam','Estimated Data Flag - District Hot Water','Estimated Data Flag - District Chilled Water','Estimated Data Flag - Municipally Supplied Potable Water - Indoor Use','Estimated Data Flag - Municipally Supplied Potable Water - Outdoor Use','Estimated Data Flag - Municipally Supplied Potable Water: Mixed Indoor/Outdoor Use','Estimated Data Flag - Municipally Supplied Reclaimed Water - Indoor Use','Estimated Data Flag - Municipally Supplied Reclaimed Water: Mixed Indoor/Outdoor Use','PM Generation Date','PM Release Date','Updated','Created']]
	data_View['Property Name'] = data_View['Property Name'].replace(' | ',' ', regex=True)

	for index, row in SEED_data_noissues_resub.iterrows():
		data_View_2 = data_View.drop(data_View[data_View['PM Property ID'] != row['PM Property ID']].index)
		data_View_2 = data_View_2.set_index('PM Property ID')
		property_name = row['Property Name']
		property_name = property_name.replace(' | ', " ")
		property_name = property_name.replace('/', " ")
		
		

		
		data_View_2.to_excel('.\Accepted Reports\\'+SEEDcycleYear.replace('CY ','')+' District Benchmark Results and Compliance Report ('+property_name+').xlsx')
		"""
		if row['Compliant'] == 'Applied':
			pass
		else:
			is_valid = validate_email(row['Property Data Administrator - Email'], verify = True)
			if is_valid != False:
				messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Accepted Resubmission','Subject'].values[0]
				messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Accepted Resubmission','Body'].values[0]
				#messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Accepted Resubmission Post April 1st','Subject'].values[0]
				#messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Accepted Resubmission Post April 1st','Body'].values[0]
				
				messageSubject = messageSubject.replace('[REPORTEDYEAR]',SEEDcycleYear.replace('CY ',''))
				messageSubject = messageSubject.replace('[PROPERTYNAME]',row['Property Name'])			
				messageSubject = messageSubject.replace('[PMID]',row['PM Property ID'])	
				messageSubject = messageSubject.replace('[DCIDS]',str(row['District Of Columbia Real Property Unique ID']))
				messageBody = messageBody.replace('[CONTACTNAME]', row['Property Data Administrator'])
				messageBody = messageBody.replace('[DATAYEAR]',SEEDcycleYear.replace('CY ',''))		
				messageBody = messageBody.replace('[PMID]',row['PM Property ID'])	
				messageBody = messageBody.replace('[DCIDS]',str(row['District Of Columbia Real Property Unique ID']))			
				messageBody = messageBody.replace('[PROPERTYADDRESS]',row['Address line 1 - custom'])		
				messageBody = messageBody.replace('[PROPERTYNAME]',row['Property Name'])
				messageBody = messageBody.replace('[NEXTDATAYEAR]',str(int(SEEDcycleYear.replace('CY ',''))+1))																	
				m = account.new_message()
				m.sender.address = 'info.benchmark@DC.gov'
				#m.to.add('andrew.held@dc.gov')
				m.to.add(row['Property Data Administrator - Email'])
				m.to.add(row['Owner Email'])
				m.cc.add('info.benchmark@DC.gov')	
				m.subject = messageSubject
				m.body = messageBody
				m.attachments.add('.\Accepted Reports\\'+SEEDcycleYear.replace('CY ','')+' District Benchmark Results and Compliance Report ('+property_name+').xlsx')
				print (m)
				pass
				try:
					m.send()
				
					note = 'sent to:' + row['Property Data Administrator - Email'] + ' ' + row['Owner Email'] + m.body
					payload = {'name':'Automatically Created','note_type':'Note','organization_id':SEEDorgID,'text':note}
					#SEEDClient.post_property_note(row['property_view_id'],payload)
					payload2 = {'inventory_ids':[row['property_view_id']],'add_label_ids':[4687, 4672],'remove_label_ids':[]}
					SEEDClient.put_add_label(payload2)
					try:
						payload2 = {'inventory_ids':[row['taxlot_view_id']],'add_label_ids':[4672],'remove_label_ids':[]}
						SEEDClient.put_add_label_taxlot(payload2)
					except:
						pass				
				except:
					note = 'email not sent, bad email address'
					payload = {'name':'Automatically Created','note_type':'Note','organization_id':SEEDorgID,'text':note}
					#SEEDClient.post_property_note(row['property_view_id'],payload)			
					payload2 = {'inventory_ids':[row['property_view_id']],'add_label_ids':[4672, 5520],'remove_label_ids':[]}
					SEEDClient.put_add_label(payload2)		
					try:
						payload2 = {'inventory_ids':[row['taxlot_view_id']],'add_label_ids':[4672],'remove_label_ids':[]}
						SEEDClient.put_add_label_taxlot(payload2)
					except:
						pass

			else:
				pass
	"""	
	for index, row in SEED_data_noissues_noresub.iterrows():
		data_View_2 = data_View.drop(data_View[data_View['PM Property ID'] != row['PM Property ID']].index)
		data_View_2 = data_View_2.set_index('PM Property ID')
		property_name = row['Property Name']
		property_name = property_name.replace(' | ', " ")
		property_name = property_name.replace('/', " ")
		
		

		
		data_View_2.to_excel('.\Accepted Reports\\'+SEEDcycleYear.replace('CY ','')+' District Benchmark Results and Compliance Report ('+property_name+').xlsx')
		
		
		
		is_valid = validate_email(row['Property Data Administrator - Email'], verify = True)
		if is_valid != False:
			#row.to_excel('Output\\Accepted_Report_'+tstamp+'.xlsx')	
			
			messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Accepted','Subject'].values[0]
			messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Accepted','Body'].values[0]	
			#messageSubject = emailBodys.loc[emailBodys['Email Type'] == 'Accepted Pre April 1st','Subject'].values[0]
			#messageBody = emailBodys.loc[emailBodys['Email Type'] == 'Accepted Pre April 1st','Body'].values[0]
			
			messageSubject = messageSubject.replace('[DATAYEAR]',SEEDcycleYear.replace('CY ',''))
			messageSubject = messageSubject.replace('[PROPERTYNAME]',row['Property Name'])			
			messageSubject = messageSubject.replace('[PMID]',row['PM Property ID'])	
			messageSubject = messageSubject.replace('[DCIDS]',str(row['District Of Columbia Real Property Unique ID']))
			messageBody = messageBody.replace('[CONTACTNAME]', row['Property Data Administrator'])
			messageBody = messageBody.replace('[DATAYEAR]',SEEDcycleYear.replace('CY ',''))		
			messageBody = messageBody.replace('[PMID]',row['PM Property ID'])	
			messageBody = messageBody.replace('[DCIDS]',str(row['District Of Columbia Real Property Unique ID']))			
			messageBody = messageBody.replace('[PROPERTYADDRESS]',row['Address line 1 - custom'])		
			messageBody = messageBody.replace('[PROPERTYNAME]',row['Property Name'])	
			messageBody = messageBody.replace('[NEXTDATAYEAR]',str(int(SEEDcycleYear.replace('CY ',''))+1))																
			m = account.new_message()
			m.sender.address = 'info.benchmark@DC.gov'
			#m.to.add('andrew.held@dc.gov')
			m.to.add(row['Property Data Administrator - Email'])
			m.to.add(row['Owner Email'])
			m.cc.add('info.benchmark@DC.gov')	
			m.subject = messageSubject
			m.body = messageBody
			m.attachments.add('.\Accepted Reports\\'+SEEDcycleYear.replace('CY ','')+' District Benchmark Results and Compliance Report ('+property_name+').xlsx')
			print (m)
			pass
			try:
				m.send()
			
				note = 'sent to:' + row['Property Data Administrator - Email'] + ' ' + row['Owner Email'] + m.body
				payload = {'name':'Automatically Created','note_type':'Note','organization_id':SEEDorgID,'text':note}
				#SEEDClient.post_property_note(row['property_view_id'],payload)
				payload2 = {'inventory_ids':[row['property_view_id']],'add_label_ids':[4687, 4672],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload2)
				try:
					payload2 = {'inventory_ids':[row['taxlot_view_id']],'add_label_ids':[4672],'remove_label_ids':[]}
					SEEDClient.put_add_label_taxlot(payload2)
				except:
					pass				
			except:
				note = 'email not sent, bad email address'
				payload = {'name':'Automatically Created','note_type':'Note','organization_id':SEEDorgID,'text':note}
				#SEEDClient.post_property_note(row['property_view_id'],payload)			
				payload2 = {'inventory_ids':[row['property_view_id']],'add_label_ids':[4672, 5520],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload2)		
				try:
					payload2 = {'inventory_ids':[row['taxlot_view_id']],'add_label_ids':[4672],'remove_label_ids':[]}
					SEEDClient.put_add_label_taxlot(payload2)
				except:
					pass				

		else:
			pass

	### Applies labels + Add note
	"""
	for item in SEED_data_issues_resub['property_view_id']:
		payload2 = {'inventory_ids':[item],'add_label_ids':[4687],'remove_label_ids':[]}
		SEEDClient.put_add_label(payload2)
	for item in SEED_data_issues_noresub['property_view_id']:
		payload2 = {'inventory_ids':[item],'add_label_ids':[4687],'remove_label_ids':[]}
		SEEDClient.put_add_label(payload2)	
	for item in SEED_data_noissues_resub['property_view_id']:
		payload2 = {'inventory_ids':[item],'add_label_ids':[4687, 4672],'remove_label_ids':[]}
		SEEDClient.put_add_label(payload2)
	for item in SEED_data_noissues_noresub['property_view_id']:
		payload2 = {'inventory_ids':[item],'add_label_ids':[4687, 4672],'remove_label_ids':[]}
		SEEDClient.put_add_label(payload2)

	for item in SEED_data_noissues_resub['taxlot_view_id']:
		try:
			payload2 = {'inventory_ids':[item],'add_label_ids':[4672],'remove_label_ids':[]}
			SEEDClient.put_add_label_taxlot(payload2)
		except:
			pass
	for item in SEED_data_noissues_noresub['taxlot_view_id']:
		try:
			payload2 = {'inventory_ids':[item],'add_label_ids':[4672],'remove_label_ids':[]}
			SEEDClient.put_add_label_taxlot(payload2)		
		except:
			pass
	"""

	sys.exit(main(sys.argv))
