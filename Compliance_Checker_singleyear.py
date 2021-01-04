#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Compliance_Checker_singleyear.py
#  
#  Copyright 2020 Andrew.Held <Andrew.Held@DOEE-1Z836H2>
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


class Compliance_checker(object):

	def __init__(self, SEEDUserName = None, SEEDPassword = None, SEEDorgID = None, SEEDcycleID = None, SEEDcycleYear=None):
		self.SEEDUserName = SEEDUserName
		self.SEEDPassword = SEEDPassword
		self.SEEDorgID = SEEDorgID
		self.SEEDcycleID = SEEDcycleID
		self.SEEDcycleYear = SEEDcycleYear
		
	def compliance_check(self):	
		from SEEDAPI import SEEDClient, SEEDClientv2_1 
		from ast import literal_eval
		import numpy as np
		import pandas as pd
		from datetime import timedelta
		from email.mime.text import MIMEText
		import requests, json, logging, time, smtplib, csv, xlrd, array
	
		def createFolder(directory):
			try:
				if not os.path.exists(directory):
					os.makedirs(directory)
			except OSError:
				print ('Error: Creating directory. ' +  directory)
				createFolder('./Output/')
		ts =  time.gmtime()
		tstamp =  (time.strftime("%Y%m%d_%H%M%S", ts))
		tstamp2 = (time.strftime("%m/%d/%y %H:%M:%S %p", ts))

		#SEED Log-in
		SEEDClient = SEEDClient(self.SEEDUserName, self.SEEDPassword, self.SEEDorgID)
		SEEDClientv2_1 = SEEDClientv2_1(self.SEEDUserName, self.SEEDPassword, self.SEEDorgID)
		# pulls labels
		
		SEED_Property_Label_List = json.loads(SEEDClient.post_property_filter_list().text)
		Property_label_Lookup_temp1 = []
		Property_label_Lookup_temp2 = []
		for item in SEED_Property_Label_List:
			if item['color'] == 'blue' or item['color'] == 'gray': 
				Property_label_Lookup_temp1.append(item['name'])
				Property_label_Lookup_temp2.append(item['is_applied'])
		Property_Identifier_Lookup = dict(zip(Property_label_Lookup_temp1,Property_label_Lookup_temp2))
		
		#fixes column names
		SEED_Property_Column_Name_List = json.loads(SEEDClient.get_property_column_names().text)
		SEED_Property_Column_Name_List = SEED_Property_Column_Name_List['columns']
		Property_Column_Name_Lookup_temp1 = []
		Property_Column_Name_Lookup_temp2 = []
		for item in SEED_Property_Column_Name_List:
			Property_Column_Name_Lookup_temp1.append(item['name'])
			Property_Column_Name_Lookup_temp2.append(item['display_name'])
		Property_Column_Name_Lookup = dict(zip(Property_Column_Name_Lookup_temp1,Property_Column_Name_Lookup_temp2))
		
		SEEDcycleList = json.loads(SEEDClient.get_cycles().text)
		SEEDcycleList = SEEDcycleList['cycles']

		
		for item in SEEDcycleList:
			if item['name'] == self.SEEDcycleYear:
				SEEDcycleID = str(item['id'])
		
		#pulls data from SEED
		data_View = json.loads(SEEDClient.post_property_list_default_view(self.SEEDcycleID).text)
		data_View2 = pd.json_normalize(data_View['results'])
		data_View3 = pd.json_normalize(data_View['results'],record_path='related',meta = 'property_view_id',record_prefix = '')
		#data_View3.to_excel('Output\\test'+tstamp+'_.xlsx')
		data_View4 = pd.merge(data_View2, data_View3, how='left',on='property_view_id') 
		#data_View4.to_excel('Output\\SEED_PropertyTable_Download_'+tstamp+'_.xlsx')		
		data_View4.drop(columns=['related'], inplace = True)
		data_View4.drop_duplicates(subset='property_view_id',inplace = True)
		
		for col in data_View4.columns:
			if col in Property_Column_Name_Lookup.keys():
				data_View4.rename(columns={col:Property_Column_Name_Lookup.get(col)},inplace = True)
		
		def add_identifier_labels(c,key):
			if c['property_view_id'] in Property_Identifier_Lookup[key]:
				return 'Applied'
			else:
				return 'Not Applied'      			
		
		for key in Property_Identifier_Lookup.keys():
			data_View4[key] = data_View4.apply(add_identifier_labels,args=(key,),axis = 1)
		
		data_View4 = data_View4.drop(data_View4[data_View4['Compliant'] == 'Not Applied' ].index)	
		
		return data_View4['PM Property ID'].tolist()
