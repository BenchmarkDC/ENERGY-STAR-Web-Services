from SEEDAPI import SEEDClient
from ast import literal_eval
import numpy as np
import pandas as pd
from datetime import timedelta
from carto.auth import APIKeyAuthClient
from carto.datasets import DatasetManager
import requests, json, logging, time, smtplib, os, csv, xlrd, array


filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Mapupload.log')
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

liveEnvironmentAccounts = pd.read_excel(open('WebServices_Reference_Document.XLSX', 'rb'), sheet_name='liveEnvironmentAccounts', dtype = str)
SEEDUserName = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='SEED','Account UserName'].values[0])
SEEDPassword = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='SEED','Account Password'].values[0])
SEEDorgID = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='SEED','Org Info'].values[0])
CartoUserName = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='Carto','Account UserName'].values[0])
CartoPassword = str(liveEnvironmentAccounts.loc[liveEnvironmentAccounts['Account Name']=='Carto','Account Password'].values[0])
SEEDClient = SEEDClient(SEEDUserName, SEEDPassword, '313')


#gets names of columns
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

#Downloads benchmarking data

data_View = json.loads(SEEDClient.get_taxlot_list('318').text)
data_View2 = pd.json_normalize(data_View['results'])
data_View3 = pd.json_normalize(data_View['results'],record_path='related',meta = 'taxlot_view_id')
data_View4 = pd.merge(data_View2, data_View3, how='left',on='taxlot_view_id')

data_View4.drop(columns=['related'], inplace = True)
for col in data_View4.columns:
	if col in Property_Column_Name_Lookup.keys():
		data_View4.rename(columns={col:Property_Column_Name_Lookup.get(col)},inplace = True)

for col in data_View4.columns:
	if col in Taxlot_Column_Name_Lookup.keys():
		data_View4.rename(columns={col:Taxlot_Column_Name_Lookup.get(col)},inplace = True)


data_View4['ENERGYSTARSCORE'] = pd.to_numeric(data_View4['ENERGYSTARSCORE'],errors='coerce')
data_View4.columns = data_View4.columns.str.lower()
data_View4.columns = [col.replace(' (tax lot)', '') for col in data_View4.columns]
data_View4.columns = data_View4.columns.str.replace(' ','_')


data_View4.rename(columns={
	'dcrealpropertyid':'dc_real_pid',
	'pmpropertyid':'pm_pid',
	'propertyname':'property_name',
	'pmparentpropertyid':'pm_parent_pid',
	'parentpropertyname':'parent_property_name',
	'reportingyear':'year_ending',
	'reportstatus':'report_status',
	'addressofrecord':'address_of_record',
	'ownerofrecord':'owner_of_record',
	'reportedaddress':'reported_address',
	'postalcode':'postal_code',
	'yearbuilt':'year_built',
	'primarypropertytype_selfselect':'primary_ptype_self',
	'primarypropertytype_epacalc':'primary_ptype_epa',
	'taxrecordfloorarea':'tax_record_floor_area',
	'reportedbuildinggrossfloorarea':'reported_gross_floor_area',
	'energystarscore':'energy_star_score',
	'siteeui_kbtu_ft':'site_eui',
	'weathernormalzedsiteeui_kbtuft':'weather_norm_site_eui',
	'sourceeui_kbtu_ft':'source_eui',
	'weathernormalzedsourceeui_kbtuft':'weather_norm_source_eui',
	'totghgemissions_metrictonsco2e':'total_ghg_emissions',
	'totghgemissintensity_kgco2eft':'total_ghg_emissions_intensity',
	'wateruse_allwatersources_kgal':'water_use',
	'waterscore_mfproperties':'water_score_mf_properties',
	'electricityuse_grid_kwh':'electricity_grid_use',
	'electricityuse_renewable_kwh':'electricity_renewable_use',
	'naturalgasuse_therms':'natural_gas_use',
	'fueloilanddieselfuelusekbtu':'fuel_use',
	'distrchilledwater_kbtu':'district_chilled_water_use',
	'distrhotwater_kbtu':'district_hot_water_use',
	'distrsteam_kbtu':'district_steam_use',
	'x':'latitude',
	'y':'longitude'}, inplace= True)


benchmarking_disclosure = data_View4[['pid', 'property_name', 'dc_real_pid', 'ubid', 'pm_pid', 'pm_parent_pid', 'parent_property_name', 'year_ending', 'report_status', 'address_of_record', 'owner_of_record', 'ward', 'reported_address', 'city', 'state', 'postal_code', 'year_built', 'primary_ptype_self', 'primary_ptype_epa', 'tax_record_floor_area', 'reported_gross_floor_area', 'energy_star_score', 'site_eui', 'weather_norm_site_eui', 'source_eui', 'weather_norm_source_eui', 'total_ghg_emissions', 'total_ghg_emissions_intensity', 'water_score_mf_properties', 'water_use', 'electricity_grid_use', 'electricity_renewable_use', 'natural_gas_use', 'fuel_use', 'district_steam_use', 'district_hot_water_use', 'district_chilled_water_use', 'latitude', 'longitude']]
benchmarking_disclosure = benchmarking_disclosure[(benchmarking_disclosure['year_ending'] >= 2012 ) & (benchmarking_disclosure['year_ending'].notnull())]
#benchmarking_disclosure.set_index('pid')
#Downloads BEPS data

data_View  = json.loads(SEEDClient.get_taxlot_list('416').text)
data_View2 = pd.json_normalize(data_View['results'])
data_View3 = pd.json_normalize(data_View['results'],record_path='related',meta = 'taxlot_view_id')
data_View4 = pd.merge(data_View2, data_View3, how='left',on='taxlot_view_id')

data_View4.drop(columns=['related'], inplace = True)
for col in data_View4.columns:
	if col in Property_Column_Name_Lookup.keys():
		data_View4.rename(columns={col:Property_Column_Name_Lookup.get(col)},inplace = True)

for col in data_View4.columns:
	if col in Taxlot_Column_Name_Lookup.keys():
		data_View4.rename(columns={col:Taxlot_Column_Name_Lookup.get(col)},inplace = True)

data_View4.columns = data_View4.columns.str.lower()
data_View4.columns = [col.replace(' (tax lot)', '') for col in data_View4.columns]
data_View4.columns = data_View4.columns.str.replace(' ','_')


data_View4.rename(columns={
	'dcrealpropertyid':'dc_real_pid',
	'pmpropertyid':'pm_pid'}, inplace= True)

beps_disclosure = data_View4[['pid','dc_real_pid','pm_pid','beps','beps_metric_type','beps_property_group','property_beps_metric_year','property_beps_metric','meets_beps','distance_from_beps_estimated','performance_requirement_est' ]]

#merge datasets Do two seperate merges and then combine the values where missing


#benchmarking_disclosure = pd.merge(benchmarking_disclosure, beps_disclosure, how='outer', on= ['pm_pid', 'dc_real_pid'])
benchmarking_disclosure_1 = pd.merge(benchmarking_disclosure, beps_disclosure, how='left', on='pid',suffixes=['_x','_y'])
benchmarking_disclosure = pd.merge(benchmarking_disclosure_1, beps_disclosure, how='left', left_on='dc_real_pid_x',right_on = 'dc_real_pid',suffixes=['_a','_c'])


benchmarking_disclosure['beps'] = benchmarking_disclosure['beps_a'].combine_first(benchmarking_disclosure['beps_c'])
benchmarking_disclosure['beps_metric_type'] = benchmarking_disclosure['beps_metric_type_a'].combine_first(benchmarking_disclosure['beps_metric_type_c'])
benchmarking_disclosure['beps_property_group'] = benchmarking_disclosure['beps_property_group_a'].combine_first(benchmarking_disclosure['beps_property_group_c'])
benchmarking_disclosure['property_beps_metric_year'] = benchmarking_disclosure['property_beps_metric_year_a'].combine_first(benchmarking_disclosure['property_beps_metric_year_c'])
benchmarking_disclosure['property_beps_metric'] = benchmarking_disclosure['property_beps_metric_a'].combine_first(benchmarking_disclosure['property_beps_metric_c'])
benchmarking_disclosure['meets_beps'] = benchmarking_disclosure['meets_beps_a'].combine_first(benchmarking_disclosure['meets_beps_c'])
benchmarking_disclosure['distance_from_beps_estimated'] = benchmarking_disclosure['distance_from_beps_estimated_a'].combine_first(benchmarking_disclosure['distance_from_beps_estimated_c'])
benchmarking_disclosure['distance_from_beps_estimated'] = pd.to_numeric((benchmarking_disclosure['distance_from_beps_estimated']).str.replace('%',''))

benchmarking_disclosure['performance_requirement_est'] = benchmarking_disclosure['performance_requirement_est_a'].combine_first(benchmarking_disclosure['performance_requirement_est_c'])

benchmarking_disclosure = benchmarking_disclosure[['pid_a', 'property_name', 'dc_real_pid_x', 'ubid', 'pm_pid_x', 'pm_parent_pid', 'parent_property_name', 'year_ending', 'report_status', 'address_of_record', 'owner_of_record', 'ward', 'reported_address', 'city', 'state', 'postal_code', 'year_built', 'primary_ptype_self', 'primary_ptype_epa', 'tax_record_floor_area', 'reported_gross_floor_area', 'energy_star_score', 'site_eui', 'weather_norm_site_eui', 'source_eui', 'weather_norm_source_eui', 'total_ghg_emissions', 'total_ghg_emissions_intensity', 'water_score_mf_properties', 'water_use', 'electricity_grid_use', 'electricity_renewable_use', 'natural_gas_use', 'fuel_use', 'district_steam_use', 'district_hot_water_use', 'district_chilled_water_use', 'latitude', 'longitude','beps','beps_metric_type','beps_property_group','property_beps_metric_year','property_beps_metric','meets_beps','distance_from_beps_estimated','performance_requirement_est']]

benchmarking_disclosure.rename(columns={
	'pid_a':'pid',
	'dc_real_pid_x':'dc_real_pid',
	'pm_pid_x': 'pm_pid'}, inplace= True)
benchmarking_disclosure.drop_duplicates(['pid', 'year_ending'], inplace= True)

#Calculate fields

benchmarking_disclosure['electricity_grid_use'] = benchmarking_disclosure['electricity_grid_use'].fillna(0)
benchmarking_disclosure['natural_gas_use'] = benchmarking_disclosure['natural_gas_use'].fillna(0)
benchmarking_disclosure['reported_gross_floor_area'] = benchmarking_disclosure['reported_gross_floor_area'].fillna(0)
benchmarking_disclosure['district_energy_use'] = benchmarking_disclosure['district_hot_water_use'] + benchmarking_disclosure['district_chilled_water_use'] + benchmarking_disclosure['district_steam_use']


benchmarking_disclosure['estimated_annual_cost_total'] = benchmarking_disclosure.apply(lambda row: row['electricity_grid_use']*0.13 + row['natural_gas_use']*1.18 if row['report_status'] == 'In Compliance' else 0, axis = 1).fillna(0)
benchmarking_disclosure['estimated_annual_cost_ft'] = benchmarking_disclosure.apply(lambda row: row['estimated_annual_cost_total']/row['reported_gross_floor_area']  if row['report_status'] == 'In Compliance' and row['reported_gross_floor_area'] != 0 else 0, axis = 1).fillna(0)

benchmarking_disclosure['percent_electric_consumption'] = benchmarking_disclosure.apply(lambda row: ((row['electricity_grid_use'] + row['electricity_renewable_use'])*3.412 )/ ((row['electricity_grid_use'] + row['electricity_renewable_use'])*3.412  + (row['natural_gas_use'] *100))  if row['report_status'] == 'In Compliance' and row['electricity_grid_use'] != 0 else 0, axis = 1).fillna(0)
benchmarking_disclosure['percent_gas_consumption'] = benchmarking_disclosure.apply(lambda row: (row['natural_gas_use'] *100)/ ((row['electricity_grid_use'] + row['electricity_renewable_use'])*3.412  + (row['natural_gas_use'] *100))  if row['report_status'] == 'In Compliance' and row['natural_gas_use'] != 0 else 0, axis = 1).fillna(0)


benchmarking_disclosure['percent_electric_emissions'] = benchmarking_disclosure.apply(lambda row: (row['electricity_grid_use'] * 3.412/1000*92.85/1000)/ ((row['electricity_grid_use'] * 3.412/1000*92.85/1000)+(row['natural_gas_use'] * 100/1000*53.11/1000))  if row['report_status'] == 'In Compliance' and  row['electricity_grid_use'] != 0 else 0, axis = 1).fillna(0)
benchmarking_disclosure['percent_gas_emissions'] = benchmarking_disclosure.apply(lambda row: (row['natural_gas_use'] * 100/1000*53.11/1000)/ ((row['electricity_grid_use'] * 3.412/1000*92.85/1000)+(row['natural_gas_use'] * 100/1000*53.11/1000)) if row['report_status'] == 'In Compliance' and row['natural_gas_use'] != 0 else 0, axis = 1).fillna(0)

#def metric_line_up (data, year, metric, backup_metric, metric_name, columns):
def metric_line_up(data, year, metric, metric_name, columns):
	columns.append(metric_name)
	temp = data[(data['year_ending'] == year)]
	#temp = temp[['pid', 'dc_real_pid', metric, backup_metric]]
	temp = temp[['pid', 'dc_real_pid', metric]]
	data = pd.merge(data, temp, how='left', on='pid',
									   suffixes=['_d', '_e'])
	data = pd.merge(data, temp, how='left', left_on='dc_real_pid_d',
									   right_on='dc_real_pid', suffixes=['_f', '_g'])
	data[metric_name] = data[metric+'_e'].combine_first(data[metric])
	#data[backup_metric] = data[backup_metric + '_e'].combine_first(data[backup_metric])

	data = data[columns_keep]
	data.rename(columns={
		'pid_f': 'pid',
		'dc_real_pid_d': 'dc_real_pid',
		metric+'_d': metric}, inplace=True)
	data.drop_duplicates(['pid', 'year_ending'], inplace=True)
	return data

columns_keep = ['pid_f', 'property_name', 'dc_real_pid_d', 'ubid', 'pm_pid', 'pm_parent_pid', 'parent_property_name', 'year_ending', 'report_status', 'address_of_record', 'owner_of_record', 'ward', 'reported_address', 'city', 'state', 'postal_code', 'year_built', 'primary_ptype_self', 'primary_ptype_epa', 'tax_record_floor_area', 'reported_gross_floor_area', 'energy_star_score', 'site_eui', 'weather_norm_site_eui_d', 'source_eui', 'weather_norm_source_eui', 'total_ghg_emissions', 'total_ghg_emissions_intensity', 'water_score_mf_properties', 'water_use', 'electricity_grid_use', 'electricity_renewable_use', 'natural_gas_use', 'fuel_use', 'district_steam_use', 'district_hot_water_use', 'district_chilled_water_use', 'latitude', 'longitude','beps','beps_metric_type','beps_property_group','property_beps_metric_year','property_beps_metric','meets_beps','distance_from_beps_estimated','performance_requirement_est','estimated_annual_cost_total', 'estimated_annual_cost_ft', 'percent_electric_consumption', 'percent_gas_consumption', 'percent_electric_emissions', 'percent_gas_emissions','district_energy_use']
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2020,'weather_norm_site_eui','weather_norm_site_eui_2020',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2019,'weather_norm_site_eui','weather_norm_site_eui_2019',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2018,'weather_norm_site_eui','weather_norm_site_eui_2018',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2017,'weather_norm_site_eui','weather_norm_site_eui_2017',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2016,'weather_norm_site_eui','weather_norm_site_eui_2016',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2015,'weather_norm_site_eui','weather_norm_site_eui_2015',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2014,'weather_norm_site_eui','weather_norm_site_eui_2014',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2013,'weather_norm_site_eui','weather_norm_site_eui_2013',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2012,'weather_norm_site_eui','weather_norm_site_eui_2012',columns_keep)


columns_keep.remove('weather_norm_site_eui_d')
columns_keep.remove('weather_norm_source_eui')
columns_keep.append('weather_norm_site_eui')
columns_keep.append('weather_norm_source_eui_d')


benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2020,'weather_norm_source_eui','weather_norm_source_eui_2020',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2019,'weather_norm_source_eui','weather_norm_source_eui_2019',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2018,'weather_norm_source_eui','weather_norm_source_eui_2018',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2017,'weather_norm_source_eui','weather_norm_source_eui_2017',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2016,'weather_norm_source_eui','weather_norm_source_eui_2016',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2015,'weather_norm_source_eui','weather_norm_source_eui_2015',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2014,'weather_norm_source_eui','weather_norm_source_eui_2014',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2013,'weather_norm_source_eui','weather_norm_source_eui_2013',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2012,'weather_norm_source_eui','weather_norm_source_eui_2012',columns_keep)


columns_keep.remove('weather_norm_source_eui_d')
columns_keep.remove('total_ghg_emissions_intensity')
columns_keep.append('weather_norm_source_eui')
columns_keep.append('total_ghg_emissions_intensity_d')

benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2020,'total_ghg_emissions_intensity','total_ghg_emissions_intensity_2020',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2019,'total_ghg_emissions_intensity','total_ghg_emissions_intensity_2019',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2018,'total_ghg_emissions_intensity','total_ghg_emissions_intensity_2018',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2017,'total_ghg_emissions_intensity','total_ghg_emissions_intensity_2017',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2016,'total_ghg_emissions_intensity','total_ghg_emissions_intensity_2016',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2015,'total_ghg_emissions_intensity','total_ghg_emissions_intensity_2015',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2014,'total_ghg_emissions_intensity','total_ghg_emissions_intensity_2014',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2013,'total_ghg_emissions_intensity','total_ghg_emissions_intensity_2013',columns_keep)
benchmarking_disclosure = metric_line_up(benchmarking_disclosure,2012,'total_ghg_emissions_intensity','total_ghg_emissions_intensity_2012',columns_keep)

benchmarking_disclosure['site_eui_change_current_last'] = benchmarking_disclosure.apply(lambda row: (row['weather_norm_site_eui_'+str(row['year_ending']).replace('.0','')] - row['weather_norm_site_eui_'+str(max(row['year_ending']-5,2012)).replace('.0','')])/row['weather_norm_site_eui_'+str(row['year_ending']).replace('.0','')] if row['year_ending'] > 2012 and row['weather_norm_site_eui_'+str(row['year_ending']).replace('.0','')] != 0 and pd.notnull(row['weather_norm_site_eui_'+str(row['year_ending']).replace('.0','')])  else 0, axis = 1)
benchmarking_disclosure['source_eui_change_current_last'] = benchmarking_disclosure.apply(lambda row: (row['weather_norm_source_eui_'+str(row['year_ending']).replace('.0','')] - row['weather_norm_source_eui_'+str(max(row['year_ending']-5,2012)).replace('.0','')])/row['weather_norm_source_eui_'+str(row['year_ending']).replace('.0','')] if row['year_ending'] > 2012 and row['weather_norm_source_eui_'+str(row['year_ending']).replace('.0','')] != 0 else 0, axis = 1)
benchmarking_disclosure['tot_ghg_emissions_intensity_change_current_last'] = benchmarking_disclosure.apply(lambda row: (row['total_ghg_emissions_intensity_'+str(row['year_ending']).replace('.0','')] - row['total_ghg_emissions_intensity_'+str(max(row['year_ending']-5,2012)).replace('.0','')])/row['total_ghg_emissions_intensity_'+str(row['year_ending']).replace('.0','')] if row['year_ending'] > 2012 and row['total_ghg_emissions_intensity_'+str(row['year_ending']).replace('.0','')] != 0 else 0, axis = 1)

benchmarking_disclosure = benchmarking_disclosure[['pid', 'property_name', 'dc_real_pid', 'ubid', 'pm_pid', 'pm_parent_pid', 'parent_property_name', 'year_ending', 'report_status', 'address_of_record', 'owner_of_record', 'ward', 'reported_address', 'city', 'state', 'postal_code', 'year_built', 'primary_ptype_self', 'primary_ptype_epa', 'tax_record_floor_area', 'reported_gross_floor_area', 'energy_star_score', 'site_eui', 'weather_norm_site_eui', 'source_eui', 'weather_norm_source_eui', 'total_ghg_emissions', 'total_ghg_emissions_intensity', 'water_score_mf_properties', 'water_use', 'electricity_grid_use', 'electricity_renewable_use', 'natural_gas_use', 'fuel_use', 'district_steam_use', 'district_hot_water_use', 'district_chilled_water_use', 'district_energy_use', 'latitude', 'longitude','beps','beps_metric_type','beps_property_group','property_beps_metric_year','property_beps_metric','meets_beps','distance_from_beps_estimated','performance_requirement_est','estimated_annual_cost_total', 'estimated_annual_cost_ft', 'percent_electric_consumption', 'percent_gas_consumption', 'percent_electric_emissions', 'percent_gas_emissions','weather_norm_site_eui_2020','weather_norm_site_eui_2019','weather_norm_site_eui_2018','weather_norm_site_eui_2017','weather_norm_site_eui_2016','weather_norm_site_eui_2015','weather_norm_site_eui_2014','weather_norm_site_eui_2013','weather_norm_site_eui_2012','site_eui_change_current_last','weather_norm_source_eui_2020','weather_norm_source_eui_2019','weather_norm_source_eui_2018','weather_norm_source_eui_2017','weather_norm_source_eui_2016','weather_norm_source_eui_2015','weather_norm_source_eui_2014','weather_norm_source_eui_2013','weather_norm_source_eui_2012','source_eui_change_current_last','total_ghg_emissions_intensity_2020','total_ghg_emissions_intensity_2019','total_ghg_emissions_intensity_2018','total_ghg_emissions_intensity_2017','total_ghg_emissions_intensity_2016','total_ghg_emissions_intensity_2015','total_ghg_emissions_intensity_2014','total_ghg_emissions_intensity_2013','total_ghg_emissions_intensity_2012','tot_ghg_emissions_intensity_change_current_last']]
benchmarking_disclosure.set_index('pid', inplace = True)
#benchmarking_disclosure.to_excel('Output\\tbl_Consolidated_2012_infinite.xlsx')
benchmarking_disclosure.to_csv('Output\\tbl_Consolidated_2012_infinite.csv')

#Saves data

CartoUser = CartoUserName
USR_BASE_URL = "https://{user}.carto.com/".format(user=CartoUser)
auth_client = APIKeyAuthClient(api_key = CartoPassword,base_url = USR_BASE_URL)

dataset_manager = DatasetManager(auth_client)
datasets = dataset_manager.get('tbl_consolidated_2012_infinite')
datasets.delete()
dataset = dataset_manager.create('Output\\tbl_Consolidated_2012_infinite.csv')
