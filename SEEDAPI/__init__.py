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
        self.domain = "https://seedv2.lbl.gov/api/v2/"
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
    def post_newfile(self,import_record,payload):
        resource = self.domain+"upload/?import_record="+import_record
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), files = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    def post_save_newfile(self,datafile_record,payload):
        resource = self.domain+"/import_files/"+datafile_record+"/save_raw_data/"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
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
        resource = self.domain+"labels/filter/?inventory_type=property&organization_id=" +self.orgID
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
    def post_portolio_manager_save_data(self,payload):
        resource = self.domain+"/upload/create_from_pm_import/"
        response = requests.post(resource, auth = HTTPBasicAuth(self.username, self.password), json = payload)
        
        if response.status_code != 200:
            return _raise_for_status(response)
        return response 

class SEEDClientv2_1(object):
    def __init__(self, username=None, password=None, orgID = None):
        if username is None or password is None:
            raise Exception("Username and Password required")
        self.domain = "https://seedv2.lbl.gov/api/v2_1/"
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
