#####
#
# SEED API
#
#####
import requests, json
from requests.auth import HTTPBasicAuth
import datetime
import array
import xml.etree.ElementTree as Et
from os.path import join, abspath

class SEEDClient(object):
    def __init__(self, username=None, password=None, orgID = None):
        if username is None or password is None:
            raise Exception("Username and Password required")
        self.domain = "https://seedv2.lbl.gov/api/v3/"
        #self.domain = "https://seeddemostaging.lbl.gov/api/v3/"
        self.username = username
        self.password = password
        self.orgID = orgID


    def get_organizations(self):
        resource = self.domain + "organizations/"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_cycles(self):
        resource = self.domain+"cycles/?organization_id="+self.orgID
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response 
    def get_taxlots(self,cycle):
        resource = self.domain+"taxlots/?organization_id="+self.orgID+"&cycle="+cycle
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_datasets(self):
        resource = self.domain+"datasets/?organization_id="+self.orgID
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_projects(self):
        resource = self.domain+"projects/?organization_id="+self.orgID
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response 
    def post_dataset(self,payload):
        resource = self.domain+"datasets/?organization_id="+self.orgID
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), data = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_newfile(self, payload_file, payload_data):
		#Does this need the "?organization_id="+self.orgID"???
        resource = self.domain+"upload/?organization_id="+self.orgID
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), files = payload_file, data = payload_data)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_save_newfile(self,datafile_record,payload):
        resource = self.domain+"/import_files/"+datafile_record+"/start_save_data/?organization_id="+self.orgID
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_save_newfile_progress(self, import_id):
        resource = self.domain+"progress/:1:SEED:save_raw_data:PROG:"+import_id+"/"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_property_info(self,property_id):
        resource = self.domain+"properties/"+property_id+"/?organization_id=" + self.orgID
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_property_filter_list(self):
        resource = self.domain+"properties/labels/?organization_id=" +self.orgID
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_property_list(self,cycle_Id):
        resource = self.domain+"properties/filter/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=99999999"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response 
    def post_property_list_custom(self,payload, cycle_Id):
        resource = self.domain+"properties/filter/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=99999999"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response     
    def post_taxlot_list(self,cycle_Id):
        resource = self.domain+"taxlots/filter/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=99999999"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_portolio_manager_save_data(self,payload):
        resource = self.domain+"/upload/create_from_pm_import/"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response 
    def post_data_quality_checks(self,payload):
        resource = self.domain+"data_quality_checks/?organization_id="+self.orgID

        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_data_quality_progress(self,PROGID):
        resource = self.domain+"progress/"+PROGID
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def put_add_label(self,payload):
        resource = self.domain+"labels_property/?organization_id="+self.orgID
        response = requests.put(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def put_add_label_taxlot(self,payload):
        resource = self.domain+"labels_taxlot/?organization_id="+self.orgID
        response = requests.put(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_property_column_names(self):
        resource = self.domain+"columns/?organization_id="+self.orgID+"&inventory_type=property"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_taxlot_column_names(self):
        resource = self.domain+"columns/?organization_id="+self.orgID+"&inventory_type=taxlot"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
        
    def post_taxlot_label_list(self):
        resource = self.domain+"taxlots/labels/?organization_id=" +self.orgID
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def delete_properties(self,payload):
        resource = self.domain+"properties/batch_delete/?organization_id=" +self.orgID
        response = requests.delete(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)

        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def delete_taxlots(self,payload):
        resource = self.domain+"taxlots/batch_delete/?organization_id=" +self.orgID
        response = requests.delete(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)

        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_save_column_mappings(self,import_id,payload):
        resource = self.domain+"organizations/"+self.orgID+"/column_mappings/?import_file_id=" +import_id
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password),json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_perform_mapping(self,import_id,payload):
        resource = self.domain+"import_files/"+import_id+"/map/?organization_id=" +self.orgID
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password),json = payload)      
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_perform_mapping_status(self,import_id):
        resource = self.domain+"progress/:1:SEED:map_data:PROG:"+import_id+"/"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))      
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_mapping_results(self,import_id,alt_orgid):
        resource = self.domain+"import_files/"+import_id+"/filtered_mapping_results/?organization_id="+alt_orgid
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_perform_mapping_stop(self,import_id,alt_orgid):
		#Need to pass org ID here because I made this command variable for the whole disclosure workflow... but need to confirm
        resource = self.domain+"import_files/"+import_id+"/mapping_done/?organization_id="+alt_orgid
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_data_quality_check_import(self,import_id,alt_orgid):
        resource = self.domain+"import_files/"+import_id+"/start_data_quality_checks/?organization_id="+alt_orgid
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_data_quality_check_import_status(self,import_id):
        resource = self.domain+"progress/:1:SEED:check_data:PROG:"+import_id+"/"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_start_matching(self,import_id):
        resource = self.domain+"import_files/"+import_id+"/start_system_matching_and_geocoding/?organization_id="+self.orgID
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_start_matching_status(self,import_id):
        resource = self.domain+"progress/:1:SEED:match_buildings:PROG:"+import_id+"/"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_start_matching_results(self,import_id,alt_orgid):
        resource = self.domain+"import_files/"+import_id+"/matching_and_geocoding_results/?organization_id="+alt_orgid

        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_property_list_default_view(self,cycle_Id):
        resource = self.domain+"properties/filter/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=99999999&profile_id=62"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_property_list_complete_view(self,cycle_Id):
        resource = self.domain+"properties/filter/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=99999999&profile_id=68"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_taxlot_list(self,cycle_Id):
        resource = self.domain+"taxlots/filter/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=99999999&profile_id=63"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_portfolio_manager_import(self,payload):
        resource = self.domain+"portfolio_manager/report/"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_portfolio_manager_list(self,payload):
        resource = self.domain+"portfolio_manager/template_list/"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_taxlot_list(self,cycle_Id):
        resource = self.domain+"taxlots/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=99999999"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_property_list(self,cycle_Id):
        resource = self.domain+"properties/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=2000&profile_id=62"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_property_note(self,property_view_id,payload):
        resource = self.domain+"properties/"+str(property_view_id)+"/notes/"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response

"""
class SEEDClientv2_1(object):
    def __init__(self, username=None, password=None, orgID = None):
        if username is None or password is None:
            raise Exception("Username and Password required")
        #self.domain = "https://seedv2.lbl.gov/api/v2_1/"
        self.domain = "https://seeddemostaging.lbl.gov/api/v2_1/"
        self.username = username
        self.password = password
        self.orgID = orgID
        
    def post_portfolio_manager_import(self,payload):
        resource = self.domain+"portfolio_manager/report/"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_portfolio_manager_list(self,payload):
        resource = self.domain+"portfolio_manager/template_list/"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_taxlot_list(self,cycle_Id):
        resource = self.domain+"taxlots/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=99999999"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def get_property_list(self,cycle_Id):
        resource = self.domain+"properties/?cycle="+cycle_Id+"&organization_id="+self.orgID+"&page=1&per_page=2000&profile_id=62"
        response = requests.get(resource, auth = HTTPBasicAuth(self.username, self.password))
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_property_note(self,property_view_id,payload):
        resource = self.domain+"properties/"+str(property_view_id)+"/notes/"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response      
"""
def _raise_for_status(response):
    """
    Custom raise_for_status with more appropriate error message.
    """
    http_error_msg = ""

    if 400 <= response.status_code < 500:
        http_error_msg = "{0} Client Error: {1}".format(response.status_code,
                                                        response.reason)

    elif 500 <= response.status_code < 600:
        http_error_msg = "{0} Server Error: {1}".format(response.status_code,
                                                        response.reason)

    if http_error_msg:
        try:
            more_info = response.json().get("message")
        except ValueError:
            more_info = None
        if more_info and more_info.lower() != response.reason.lower():
            http_error_msg += ".\n\t{0}".format(more_info)
        raise requests.exceptions.HTTPError(http_error_msg, response=response)
