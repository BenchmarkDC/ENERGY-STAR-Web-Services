#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  DAta_Quality_Checks.py
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
from __main__ import *
import sys, os
import xml.etree.ElementTree as Et


class Data_quality_checker(object):

	
	
	def __init__(self, SEEDUserName = None, SEEDPassword = None, SEEDorgID = None, SEEDcycleID = None, SEEDcycleYear=None):
		self.SEEDUserName = SEEDUserName
		self.SEEDPassword = SEEDPassword
		self.SEEDorgID = SEEDorgID
		self.SEEDcycleID = SEEDcycleID
		self.SEEDcycleYear = SEEDcycleYear

	def quality_check(self):	
		from SEEDAPI import SEEDClient 
		from ast import literal_eval
		import numpy as np
		import pandas as pd
		from email.mime.text import MIMEText
		import requests, json, logging, time, smtplib, csv, xlrd, array, datetime
		
		def createFolder(directory):
			try:
				if not os.path.exists(directory):
					os.makedirs(directory)
			except OSError:
				print ('Error: Creating directory. ' +  directory)
		


		def data_center_flag(c):
			ids_to_label = c[c['Alert - Data Center Issue (with Estimates, IT Configuration, Or IT Meter)'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Data Center Issue (with Estimates, IT Configuration, Or IT Meter)'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4661],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4661]}
				SEEDClient.put_add_label(payload)
			except:
				pass

		def energy_meter_gaps_flag(c):
			ids_to_label = c[c['Alert - Energy Meter Has Gaps'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Energy Meter Has Gaps'] ==  'Ok']	
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4663],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4663]}
				SEEDClient.put_add_label(payload)	
			except:
				pass

		def energy_meter_less_than_12_months_flag(c):
			ids_to_label = c[c['Alert - Energy Meter Has Less Than 12 Full Calendar Months Of Data'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Energy Meter Has Less Than 12 Full Calendar Months Of Data'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4664],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4664]}
				SEEDClient.put_add_label(payload)	
			except:
				pass

		def energy_meter_overlaps_flag(c):
			ids_to_label = c[c['Alert - Energy Meter Has Overlaps'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Energy Meter Has Overlaps'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4665],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4665]}
				SEEDClient.put_add_label(payload)	
			except:
				pass	
		def energy_meter_large_single_entry_flag(c):
			ids_to_label = c[c['Alert - Energy Meter Has Single Entry More Than 65 Days'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Energy Meter Has Single Entry More Than 65 Days'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4660],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4660]}
				SEEDClient.put_add_label(payload)	
			except:
				pass
				
		def no_energy_meter_flag(c):
			ids_to_label = c[c['Alert - Energy - No Meters Selected For Metrics'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Energy - No Meters Selected For Metrics'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4666],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4666]}
				SEEDClient.put_add_label(payload)	
			except:
				pass
		def gross_floor_area_too_small_flag(c):
			ids_to_label = c[c['Alert - Gross Floor Area Is 0 Ft2'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Gross Floor Area Is 0 Ft2'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4662],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4662]}
				SEEDClient.put_add_label(payload)
			except:
				pass

		def no_property_use_flag(c):
			ids_to_label = c[c['Alert - Property Has No Uses'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Property Has No Uses'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4667],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4667]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def water_meter_gaps_flag(c):
			ids_to_label = c[c['Alert - Water Meter Has Gaps'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Water Meter Has Gaps'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4669],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4669]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def water_meter_less_than_12_months_flag(c):
			ids_to_label = c[c['Alert - Water Meter Has Less Than 12 Full Calendar Months Of Data'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Water Meter Has Less Than 12 Full Calendar Months Of Data'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4670],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4670]}
				SEEDClient.put_add_label(payload)	
			except:
				pass
				
		def water_meter_overlaps_flag(c):
			ids_to_label = c[c['Alert - Water Meter Has Overlaps'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Water Meter Has Overlaps'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4671],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4671]}
				SEEDClient.put_add_label(payload)	
			except:
				pass

		def no_water_meter_flag(c):
			ids_to_label = c[c['Alert - Water - No Meters Selected For Metrics'] !=  'Ok']
			ids_to_unlabel = c[c['Alert - Water - No Meters Selected For Metrics'] ==  'Ok']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4668],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4668]}
				SEEDClient.put_add_label(payload)	
			except:
				pass
				
		def default_values_flag(c):
			ids_to_label = c[(c['Default Values'] ==  'Yes') | (c['Default Values'].isnull()) ]
			ids_to_unlabel = c[c['Default Values'] ==  'No']		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4675],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4675]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		### Split into two?
		def National_median_eui_flag(c):
			ids_to_label = c[(c['% Difference From National Median Site EUI'].astype(float) <  -50) | (c['% Difference From National Median Site EUI'].astype(float) >  50)]
			ids_to_unlabel = c[(c['% Difference From National Median Site EUI'].astype(float) >=  -50) & (c['% Difference From National Median Site EUI'].astype(float) <=  50)]
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4679],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4679]}
				SEEDClient.put_add_label(payload)
			except:
				pass
		### 

		def No_electricity_flag(c):
			ids_to_label = c[(c['Electricity Use - Grid Purchase (kWh)'].isnull()) | (c['Electricity Use - Grid Purchase (kWh)'].astype(float) ==  0)]
			ids_to_unlabel = c[c['Electricity Use - Grid Purchase (kWh)'].astype(float) > 0]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4692],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4692]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def High_energystar_score_flag(c):
			ids_to_label = c[(c['ENERGY STAR Score'].notnull()) & (c['ENERGY STAR Score'].astype(float) >=  95)]
			ids_to_unlabel = c[(c['ENERGY STAR Score'].notnull()) & (c['ENERGY STAR Score'].astype(float) <  95)]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4686],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4686]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def Low_energystar_score_flag(c):
			ids_to_label = c[(c['ENERGY STAR Score'].notnull()) & (c['ENERGY STAR Score'].astype(float) <=  5)]
			ids_to_unlabel = c[(c['ENERGY STAR Score'].notnull()) & (c['ENERGY STAR Score'].astype(float) > 5)]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4691],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4691]}
				SEEDClient.put_add_label(payload)	
			except:
				pass
				
		def No_energystar_score_flag(c):
			PM_types_with_Score = ['Bank Branch','Barracks','Courthouse','Data Center','Distribution Center','Financial Office','Hospital (General Medical & Surgical)','Hotel','K-12 School','Medical Office','Multifamily Housing','Non-Refrigerated Warehouse','Office','Refrigerated Warehouse','Residence Hall/ Dormitory','Retail Store','Senior Care Community','Supermarket/Grocery Store','Wastewater Treatment Plant','Wholesale Club/Supercenter','Worship Facility']

			ids_to_label = c[(c['ENERGY STAR Score'].isnull()) & (c['Primary Property Type - Portfolio Manager-Calculated'].isin(PM_types_with_Score))]
			ids_to_unlabel = c[c['ENERGY STAR Score'].notnull() | (~c['Primary Property Type - Portfolio Manager-Calculated'].isin(PM_types_with_Score))]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4693],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4693]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def Estimated_energy_flag(c):
			ids_to_label = c[((c['Estimated Values - Energy'] != 'No') & (c['Site Energy Use (kBtu)'].notnull()))]	
			ids_to_unlabel = c[(c['Estimated Values - Energy'] == 'No') | (c['Site Energy Use (kBtu)'].isnull())]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4701],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4701]}
				SEEDClient.put_add_label(payload)	
			except:
				pass
				
		def Estimated_water_flag(c):
			ids_to_label = c[((c['Estimated Values - Water'] != 'No') & (c['Water Use (All Water Sources) (kgal)'].notnull()))]	
			ids_to_unlabel = c[(c['Estimated Values - Water'] == 'No') | (c['Water Use (All Water Sources) (kgal)'].isnull())]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4702],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4702]}
				SEEDClient.put_add_label(payload)	
			except:
				pass	
				
		def Green_power_offsite_flag(c):
			ids_to_label = c[(c['Green Power - Offsite (kWh)'].astype(float) > 0)]	
			ids_to_unlabel = c[(c['Green Power - Offsite (kWh)'].isnull()) | (c['Green Power - Offsite (kWh)'].astype(float) <= 0)]	
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4682],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4682]}
				SEEDClient.put_add_label(payload)
			except:
				pass
					
		def Green_power_onsite_flag(c):
			ids_to_label = c[(c['Green Power - Onsite (kWh)'].astype(float) > 0)]	
			ids_to_unlabel = c[(c['Green Power - Onsite (kWh)'].isnull()) | (c['Green Power - Onsite (kWh)'].astype(float) <= 0)]	
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4683],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4683]}
				SEEDClient.put_add_label(payload)	
			except:
				pass
				
		def No_irrigated_area_flag(c):
			ids_to_label = c[((c['Primary Property Type - Portfolio Manager-Calculated'] == 'Multifamily Housing') & (c['Irrigated Area (ft2)'].notnull())) | ((c['Primary Property Type - Portfolio Manager-Calculated'] == 'Multifamily Housing') & (c['Irrigated Area (ft2)'].astype(float) ==0))]	
			ids_to_unlabel = c[((c['Primary Property Type - Portfolio Manager-Calculated'] == 'Multifamily Housing') & (c['Irrigated Area (ft2)'].astype(float) > 0)) | (c['Primary Property Type - Portfolio Manager-Calculated'] != 'Multifamily Housing')]
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4700],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload = payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4700]}
				SEEDClient.put_add_label(payload)	
			except:
				pass
				
		def No_water_score_flag(c):
			ids_to_label = c[((c['Primary Property Type - Portfolio Manager-Calculated'] == 'Multifamily Housing') & (c['Water Score (Multifamily Only)'].notnull())) | ((c['Primary Property Type - Portfolio Manager-Calculated'] == 'Multifamily Housing') & (c['Water Score (Multifamily Only)'].astype(float) ==0))]	
			ids_to_unlabel = c[((c['Primary Property Type - Portfolio Manager-Calculated'] == 'Multifamily Housing') & (c['Water Score (Multifamily Only)'].astype(float) > 0)) | (c['Primary Property Type - Portfolio Manager-Calculated'] != 'Multifamily Housing')]
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4694],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4694]}
				SEEDClient.put_add_label(payload)
			except:
				pass
					
		def No_taxlot_id_flag(c):
			ids_to_label = c[c['District Of Columbia Real Property Unique ID'].isnull()]	
			ids_to_unlabel = c[c['District Of Columbia Real Property Unique ID'].notnull()]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4698],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4698]}
				SEEDClient.put_add_label(payload)		
			except:
				pass
				
		def Metered_areas_energy_flag(c):
			ids_to_label = c[(c['Metered Areas (Energy)'] != 'Whole Building') & (c['Metered Areas (Energy)'] != 'Whole Property')]
			ids_to_unlabel = c[(c['Metered Areas (Energy)'] == 'Whole Building') | (c['Metered Areas (Energy)'] == 'Whole Property')]
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4677],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4677]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def Metered_areas_water_flag(c):
			ids_to_label = c[(c['Metered Areas (Water)'] != 'Whole Building') & (c['Metered Areas (Water)'] != 'Whole Property')]
			ids_to_unlabel = c[(c['Metered Areas (Water)'] == 'Whole Building') | (c['Metered Areas (Water)'] == 'Whole Property')]
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4678],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4678]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def High_site_eui_flag(c):
			ids_to_label = c[(c['Site EUI (kBtu/ft²/year)'].notnull()) & (c['Site EUI (kBtu/ft²/year)'].astype(float) >=  500)]
			ids_to_unlabel = c[(c['Site EUI (kBtu/ft²/year)'].notnull()) & (c['Site EUI (kBtu/ft²/year)'].astype(float) <  500)]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4685],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4685]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def Low_site_eui_flag(c):
			ids_to_label = c[(c['Site EUI (kBtu/ft²/year)'].notnull()) & (c['Site EUI (kBtu/ft²/year)'].astype(float) <=  15) & (c['Site EUI (kBtu/ft²/year)'].astype(float) !=  0)]
			ids_to_unlabel = c[(c['Site EUI (kBtu/ft²/year)'].notnull()) & (c['Site EUI (kBtu/ft²/year)'].astype(float) > 15)]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4690],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4690]}
				SEEDClient.put_add_label(payload)	
			except:
				pass
				
		def No_wn_site_eui_flag(c):
			ids_to_label = c[(c['Site EUI Weather Normalized (kBtu/ft²/year)'].isnull()) | (c['Site EUI Weather Normalized (kBtu/ft²/year)'].astype(float) ==  0)]
			ids_to_unlabel = c[c['Site EUI Weather Normalized (kBtu/ft²/year)'].astype(float)  > 0]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4696],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4696]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def Percent_Electricity_odd_flag(c):
			ids_to_label = c[(c['Electricity Use - Grid Purchase And Generated From Onsite Renewable Systems (kWh)'].astype(float)*3.412)/ (c['Site Energy Use (kBtu)'].astype(float)) < .2]
			ids_to_unlabel = c[(c['Electricity Use - Grid Purchase And Generated From Onsite Renewable Systems (kWh)'].astype(float)*3.412)/ (c['Site Energy Use (kBtu)'].astype(float)) >= .2]
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[5187],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[5187]}
				SEEDClient.put_add_label(payload)
			except:
				pass	
							
		def No_water_flag(c):
			ids_to_label = c[(c['Water Use (All Water Sources) (kgal)'].isnull()) | (c['Water Use (All Water Sources) (kgal)'].astype(float) ==  0)]
			ids_to_unlabel = c[c['Water Use (All Water Sources) (kgal)'].astype(float) > 0]		
			try:
				payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4695],'remove_label_ids':[]}
				SEEDClient.put_add_label(payload)
			except:
				pass
			try:	
				payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4695]}
				SEEDClient.put_add_label(payload)
			except:
				pass
				
		def GFA_unusual_flag(c):
			try:
				ids_to_label = c[((1-abs(c['Property GFA - Calculated (Buildings) (ft2)'].astype(float)/c['SumOfGBA (Tax Lot)'].astype(float))) >=.3)]
				ids_to_unlabel = c[((1-abs(c['Property GFA - Calculated (Buildings) (ft2)'].astype(float)/c['SumOfGBA (Tax Lot)']) < .3)) | (c['SumOfGBA (Tax Lot)'].isnull())| (c['Property GFA - Calculated (Buildings) (ft2)'].isnull())]
				try:
					payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[4684],'remove_label_ids':[]}
					SEEDClient.put_add_label(payload)
				except:
					pass
				try:	
					payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[4684]}
					SEEDClient.put_add_label(payload)
				except:
					pass
			except:
				pass
		
		def property_use_details_need_update_flag(c):
			c['Last Modified Date - Property Use Details'] = pd.to_datetime(c['Last Modified Date - Property Use Details'])
			PM_types_to_check = ['Bank Branch','Courthouse','Distribution Center','Financial Office','Hospital (General Medical & Surgical)','K-12 School','Medical Office','Non-Refrigerated Warehouse','Office','Refrigerated Warehouse','Retail Store','Supermarket/Grocery Store','Wholesale Club/Supercenter','Worship Facility']
			try:
				ids_to_label = c[(c['Last Modified Date - Property Use Details'] <  pd.Timestamp(2020,12,1))&(c['Primary Property Type - Portfolio Manager-Calculated'].isin(PM_types_to_check))]
				ids_to_unlabel = c[(c['Last Modified Date - Property Use Details'] >=  pd.Timestamp(2020,12,1)) | (~c['Primary Property Type - Portfolio Manager-Calculated'].isin(PM_types_to_check))]
				try:
					payload = {'inventory_ids':ids_to_label['property_view_id'].tolist(),'add_label_ids':[5514],'remove_label_ids':[]}
					SEEDClient.put_add_label(payload)
				except:
					pass
				try:	
					payload = {'inventory_ids':ids_to_unlabel['property_view_id'].tolist(),'add_label_ids':[],'remove_label_ids':[5514]}
					SEEDClient.put_add_label(payload)
				except:
					pass
			except:
				pass
		
		
		createFolder('./Output/')
		ts =  time.gmtime()
		tstamp =  (time.strftime("%Y%m%d_%H%M%S", ts))
		tstamp2 = (time.strftime("%m/%d/%y %H:%M:%S %p", ts))

		#SEED Log-in
		SEEDClient = SEEDClient(self.SEEDUserName, self.SEEDPassword, self.SEEDorgID)

		

		
		SEED_Taxlot_Label_List = json.loads(SEEDClient.post_taxlot_label_list().text)
		Taxlot_label_Lookup_temp1 = []
		Taxlot_label_Lookup_temp2 = []
		for item in SEED_Taxlot_Label_List:
			Taxlot_label_Lookup_temp1.append(item['name'])
			Taxlot_label_Lookup_temp2.append(item['is_applied'])
		Taxlot_label_Lookup = dict(zip(Taxlot_label_Lookup_temp1,Taxlot_label_Lookup_temp2))

		SEED_Property_Label_List = json.loads(SEEDClient.post_property_filter_list().text)
		Property_label_Lookup_temp1 = []
		Property_label_Lookup_temp2 = []
		for item in SEED_Property_Label_List:
			if item['color'] == 'orange' or item['color'] == 'red': 
				Property_label_Lookup_temp1.append(item['name'])
				Property_label_Lookup_temp2.append(item['is_applied'])
		Property_label_Lookup = dict(zip(Property_label_Lookup_temp1,Property_label_Lookup_temp2))

		Property_label_Lookup_temp1 = []
		Property_label_Lookup_temp2 = []
		for item in SEED_Property_Label_List:
			if item['color'] == 'blue' or item['color'] == 'gray': 
				Property_label_Lookup_temp1.append(item['name'])
				Property_label_Lookup_temp2.append(item['is_applied'])
		Property_Identifier_Lookup = dict(zip(Property_label_Lookup_temp1,Property_label_Lookup_temp2))

		SEED_Property_Column_Name_List = json.loads(SEEDClient.get_property_column_names().text)
		SEED_Property_Column_Name_List = SEED_Property_Column_Name_List['columns']
		Property_Column_Name_Lookup_temp1 = []
		Property_Column_Name_Lookup_temp2 = []
		for item in SEED_Property_Column_Name_List:
			Property_Column_Name_Lookup_temp1.append(item['name'])
			Property_Column_Name_Lookup_temp2.append(item['display_name'])
		Property_Column_Name_Lookup = dict(zip(Property_Column_Name_Lookup_temp1,Property_Column_Name_Lookup_temp2))

		SEED_Taxlot_Column_Name_List = json.loads(SEEDClient.get_taxlot_column_names().text)
		SEED_Taxlot_Column_Name_List = SEED_Taxlot_Column_Name_List['columns']
		Taxlot_Column_Name_Lookup_temp1 = []
		Taxlot_Column_Name_Lookup_temp2 = []
		for item in SEED_Taxlot_Column_Name_List:
			Taxlot_Column_Name_Lookup_temp1.append(item['name'])
			Taxlot_Column_Name_Lookup_temp2.append(item['display_name'])
		Taxlot_Column_Name_Lookup = dict(zip(Taxlot_Column_Name_Lookup_temp1,Taxlot_Column_Name_Lookup_temp2))
		
		SEEDcycleList = json.loads(SEEDClient.get_cycles().text)
		SEEDcycleList = SEEDcycleList['cycles']

		
		for item in SEEDcycleList:
			if item['name'] == self.SEEDcycleYear:
				SEEDcycleID = str(item['id'])
		
		###
		
		### Pulls SEED Property Table
		data_View = json.loads(SEEDClient.post_property_list_default_view(self.SEEDcycleID).text)
		data_View2 = pd.io.json.json_normalize(data_View['results'])
		data_View3 = pd.io.json.json_normalize(data_View['results'],record_path='related',meta = 'property_view_id',record_prefix = '')
		#data_View3.to_excel('Output\\test'+tstamp+'_.xlsx')
		data_View4 = pd.merge(data_View2, data_View3, how='left',on='property_view_id') 
			
		data_View4.drop(columns=['related'], inplace = True)
		data_View4.drop_duplicates(subset='property_view_id',inplace = True)
		
		def add_identifier_labels(c,key):
			if c['property_view_id'] in Property_Identifier_Lookup[key]:
				return 'Applied'
			else:
				return 'Not Applied'       

		def add_quality_labels(c,key):
			if c['property_view_id'] in Property_label_Lookup[key]:
				return 'Issue'
			else:
				return 'No Issue'



		for col in data_View4.columns:
			if col in Property_Column_Name_Lookup.keys():
				data_View4.rename(columns={col:Property_Column_Name_Lookup.get(col)},inplace = True)

		for col in data_View4.columns:
			if col in Taxlot_Column_Name_Lookup.keys():
				data_View4.rename(columns={col:Taxlot_Column_Name_Lookup.get(col)},inplace = True)

		### prepares data for emails
		column_names = (list(data_View4.columns.values))
		column_names.sort()
		data_View4 = data_View4[column_names]
		data_View4.set_index('PM Property ID',inplace = True)
		data_View4.to_excel('Output\\SEED_DQC_Download_'+tstamp+'_.xlsx')	
		
		data_center_flag(data_View4)
		energy_meter_gaps_flag(data_View4)
		energy_meter_less_than_12_months_flag(data_View4)
		energy_meter_overlaps_flag(data_View4)
		energy_meter_large_single_entry_flag(data_View4)
		no_energy_meter_flag(data_View4)
		gross_floor_area_too_small_flag(data_View4)
		no_property_use_flag(data_View4)
		water_meter_gaps_flag(data_View4)
		water_meter_less_than_12_months_flag(data_View4)
		water_meter_overlaps_flag(data_View4)
		no_water_meter_flag(data_View4)
		default_values_flag(data_View4)
		National_median_eui_flag(data_View4)
		No_electricity_flag(data_View4)
		High_energystar_score_flag(data_View4)
		Low_energystar_score_flag(data_View4)
		No_energystar_score_flag(data_View4)
		Estimated_energy_flag(data_View4)
		Estimated_water_flag(data_View4)
		Green_power_offsite_flag(data_View4)
		Green_power_onsite_flag(data_View4)
		#No_irrigated_area_flag(data_View4)
		No_water_score_flag(data_View4)
		No_taxlot_id_flag(data_View4)
		Metered_areas_energy_flag(data_View4)
		Metered_areas_water_flag(data_View4)
		High_site_eui_flag(data_View4)
		Low_site_eui_flag(data_View4)
		No_wn_site_eui_flag(data_View4)
		No_water_flag(data_View4)
		#GFA_unusual_flag(data_View4)
		Percent_Electricity_odd_flag(data_View4)
		property_use_details_need_update_flag(data_View4)
		for key in Property_label_Lookup.keys():
			data_View4[key] = data_View4.apply(add_quality_labels,args=(key,),axis = 1)

		for key in Property_Identifier_Lookup.keys():
			data_View4[key] = data_View4.apply(add_identifier_labels,args=(key,),axis = 1)
			
		return data_View4		
  

