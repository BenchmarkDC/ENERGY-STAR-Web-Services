import xml.etree.ElementTree as Et
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

#Downloads data
SEEDClient = SEEDClient(SEEDUserName, SEEDPassword, '313')
data_View = json.loads(SEEDClient.get_taxlot_list('318').text)
data_View2 = pd.io.json.json_normalize(data_View['results'])
data_View3 = pd.io.json.json_normalize(data_View['results'],record_path='related',meta = 'taxlot_view_id',record_prefix = 'property.')
data_View4 = pd.merge(data_View2, data_View3, how='outer',on='taxlot_view_id') 
data_View4 = data_View4.drop_duplicates(subset='property.pm_property_id_66978')
data_View4['property.ENERGYSTARSCORE_68924'] = pd.to_numeric(data_View4['property.ENERGYSTARSCORE_68924'],errors='coerce')
data_View4.rename(columns={'property.PID_69008':'pid', 'DCREALPROPERTYID_68999':'dc_real_pid', 'property.PMPROPERTYID_69000':'pm_pid', 'property.PROPERTYNAME_68888':'property_name', 'property.PMPARENTPROPERTYID_68890':'pm_parent_pid', 'property.PARENTPROPERTYNAME_68892':'parent_property_name', 'property.REPORTINGYEAR_68896':'year_ending', 'property.REPORTSTATUS_68898':'report_status', 'ADDRESSOFRECORD_68960':'address_of_record', 'OWNEROFRECORD_68958':'owner_of_record', 'WARD_69001':'Ward', 'property.REPORTEDADDRESS_68906':'reported_address', 'CITY_69002':'city', 'STATE_69003':'state', 'POSTALCODE_69004':'postal_code', 'property.YEARBUILT_68914':'year_built', 'property.PRIMARYPROPERTYTYPE_SELFSELECT_68916':'primary_ptype_self', 'property.PRIMARYPROPERTYTYPE_EPACALC_68918':'primary_ptype_epa', 'TAXRECORDFLOORAREA_68959':'tax_record_floor_area', 'property.REPORTEDBUILDINGGROSSFLOORAREA_68922':'reported_gross_floor_area', 'property.ENERGYSTARSCORE_68924':'energy_star_score', 'property.SITEEUI_KBTU_FT_68926':'site_eui', 'property.WEATHERNORMALZEDSITEEUI_KBTUFT_68928':'weather_norm_site_eui', 'property.SOURCEEUI_KBTU_FT_68930':'source_eui', 'property.WEATHERNORMALZEDSOURCEEUI_KBTUFT_71676':'weather_norm_source_eui', 'property.TOTGHGEMISSIONS_METRICTONSCO2E_68934':'total_ghg_emissions', 'property.TOTGHGEMISSINTENSITY_KGCO2EFT_68936':'total_ghg_emissions_intensity', 'property.WATERUSE_ALLWATERSOURCES_KGAL_68938':'water_use', 'property.WATERSCORE_MFPROPERTIES_68940':'water_score_mf_properties', 'property.ELECTRICITYUSE_GRID_KWH_71442':'electricity_grid_use','property.ELECTRICITYUSE_RENEWABLE_KWH_71457':'electricity_renewable_use', 'property.NATURALGASUSE_THERMS_68944':'natural_gas_use', 'property.FUELOILANDDIESELFUELUSEKBTU_68946':'fuel_use', 'property.DISTRCHILLEDWATER_KBTU_71453':'district_chilled_water_use', 'property.DISTRHOTWATER_KBTU_71455':'district_hot_water_use', 'property.DISTRSTEAM_KBTU_71452':'district_steam_use', 'property.METEREDAREAS_ENERGY_68950':'metered_areas_energy', 'property.METEREDAREAS_WATER_68952':'metered_areas_water', 'X_68956':'latitude', 'Y_68957':'longitude'}, inplace= True)
data_View4.drop(columns =['property. ELECTRICITYUSE_RENEWABLE_KWH _71450','property. DISTRSTEAM_KBTU_71444','property. DISTRHOTWATER_KBTU _71448','property. DISTRCHILLEDWATER_KBTU _71446','property.DISTRWATER_BASEDENERGYUSE_KBTU_68948','property.ELECTRICITYUSE_KWH_68942','REPORTEDBUILDINGGROSSFLOORAREA_69005',  'address_line_1_66986', 'address_line_2_66988', 'block_number_67040', 'bounding_box', 'city_66990', 'created_67005', 'custom_id_1_66984', 'district_67041', 'geocoding_confidence_67167', 'id', 'jurisdiction_tax_lot_id_66980', 'latitude_67671', 'long_lat', 'longitude_67672', 'notes_count', 'number_properties_67039', 'postal_code_66996', 'related', 'state_66992', 'taxlot_footprint_67400', 'taxlot_state_id', 'taxlot_view_id', 'ulid_67398', 'updated_67004', 'property.ADDRESSOFRECORD_68900', 'property.CITY_68908', 'property.DCREALPROPERTYID_68885', 'property.OBJECTID_68883', 'property.OWNEROFRECORD_68902', 'property.OWNERPROVIDEDPUBLICNOTES_68954', 'property.POSTALCODE_68912', 'property.ROWCOUNT_69006', 'property.STATE_68910', 'property.TAXRECORDFLOORAREA_68920', 'property.WARD_68904', 'property.X_68879', 'property.YEARENDING_68894', 'property.Y_68881', 'property.address_line_1_66985', 'property.address_line_2_66987', 'property.analysis_end_time_67036', 'property.analysis_start_time_67035', 'property.analysis_state_67037', 'property.analysis_state_message_67038', 'property.bounding_box', 'property.building_certification_67034', 'property.building_count_67015', 'property.campus_67001', 'property.centroid', 'property.city_66989', 'property.conditioned_floor_area_67018', 'property.created_67003', 'property.custom_id_1_66983', 'property.energy_alerts_67032', 'property.energy_score_67008', 'property.generation_date_67024', 'property.geocoding_confidence_67166', 'property.gross_floor_area_67006', 'property.home_energy_score_id_67023', 'property.id', 'property.jurisdiction_property_id_66981', 'property.latitude_66999', 'property.long_lat', 'property.longitude_67000', 'property.lot_number_66997', 'property.notes_count', 'property.occupied_floor_area_67019', 'property.owner_67012', 'property.owner_address_67020', 'property.owner_city_state_67021', 'property.owner_email_67013', 'property.owner_postal_code_67022', 'property.owner_telephone_67014', 'property.pm_parent_property_id_66979', 'property.pm_property_id_66978', 'property.postal_code_66995', 'property.property.Water Score (Multifamily Only)_63501_68994', 'property.property_footprint_67399', 'property.property_name_66998', 'property.property_notes_67009', 'property.property_state_id', 'property.property_type_67010', 'property.property_view_id', 'property.recent_sale_date_67017', 'property.release_date_67025', 'property.site_eui_67026', 'property.site_eui_modeled_67028', 'property.site_eui_weather_normalized_67027', 'property.source_eui_67029', 'property.source_eui_modeled_67031', 'property.source_eui_weather_normalized_67030', 'property.space_alerts_67033', 'property.state_66991', 'property.ubid_66982', 'property.updated_67002', 'property.use_description_67007', 'property.year_built_67016', 'property.year_ending_67011' ], inplace= True)
years = data_View4['year_ending'].drop_duplicates(keep="first").tolist()

for item in years:
	data_View5 = data_View4.loc[data_View4['year_ending'] == item]
	data_View5['year_ending'] = data_View5['year_ending'].replace(str(item), '12/31/'+str(item))
	data_View5.set_index('pid')
	print (str(item).replace('.0',''))
	data_View5.to_csv('CSV output\\t'+str(item).replace('.0','')+'.csv')

#Saves data

CartoUser = CartoUserName
USR_BASE_URL = "https://{user}.carto.com/".format(user=CartoUser)
auth_client = APIKeyAuthClient(api_key = CartoPassword,base_url = USR_BASE_URL)


dataset_manager = DatasetManager(auth_client)
for item in years:
	print (item)
	try:
		datasets = dataset_manager.get('t'+str(item).replace('.0',''))
		datasets.delete()
		dataset = dataset_manager.create('CSV output\\t'+str(item).replace('.0','')+'.csv')
		print ('works')
	except:
		pass
