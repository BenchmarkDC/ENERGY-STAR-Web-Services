#####
#
# Energy Star API
#
#####
import requests
import datetime
import array
import xml.etree.ElementTree as Et
from os.path import join, abspath

class EnergyStarClient(object):
    def __init__(self, username=None, password=None):
        if username is None or password is None:
            raise Exception("Username and Password required")
        self.domain = "https://portfoliomanager.energystar.gov/ws/"
        self.username = username
        self.password = password

### Web Services Account Related Calls
    def get_account_info(self):
        resource = self.domain + "/account"
        response = requests.get(resource, auth=(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)
        return response.text
        
    def get_terms(self):
        resource = self.domain + '/dataExchangeSettings'
        response = requests.get(resource, auth=(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    
    def put_terms(self, template_file):
        if hasattr(template_file, "read"):
            resource = self.domain + '/dataExchangeSettings'
            template_info = template_file.read()
            headers = {'Content-Type': 'application/xml'}
            acct = str(template_info)
            response = requests.put(resource, data=acct, auth=(self.username, self.password), headers=headers)
            print(response.text)
            if response.status_code != 200:
                return _raise_for_status(response)
            return response.text
    
    def post_custom_fields(self, template_file):
        if hasattr(template_file, "read"):
            resource = self.domain + '/dataExchangeSettings/customField'
            template_info = template_file.read()
            headers = {'Content-Type': 'application/xml'}
            acct = str(template_info)
            response = requests.post(resource, data=acct, auth=(self.username, self.password), headers=headers)
            print(response.text)
            if response.status_code != 200:
                return _raise_for_status(response)
            return response.text

    def get_custom_fields_list(self):
        resource = self.domain + '/dataExchangeSettings/customField/list'
        response = requests.get(resource, auth=(self.username, self.password))
        if response.status_code != 200:
            return _raise_for_status(response)
        return response.text
    
    def put_update_custom_fields(self, template_file, customfieldID):
        if hasattr(template_file, "read"):
            resource = self.domain + customfieldID
            print (resource)
            template_info = template_file.read()
            headers = {'Content-Type': 'application/xml'}
            acct = str(template_info)
            response = requests.put(resource, data=acct, auth=(self.username, self.password), headers=headers)
            print(response.text)
            if response.status_code != 200:
                return _raise_for_status(response)
            return response
    def delete_custom_fields(self,customfieldID):
        resource = self.domain + customfieldID
        response = requests.delete(resource, auth=(self.username, self.password))
        if response.status_code != 200:
            return _raise_for_status(response)
        return response.text        
### Account Connection Calls
    def get_pending_connection_list_multipage(self):
        resource = self.domain +"/connect/account/pending/list"
        url = resource + "?page=1"
        page = 1
        accountIDs = []
        while url:
            print (url)
            print("Getting data from page " + str(page))
            page += 1
            response = requests.get(url, auth=(self.username, self.password))
            if response.status_code != 200:
                print(response.status_code, response.reason)
                break
            dataforlink = response.text
            root = Et.fromstring(dataforlink)
            if root.find('links') is not None:
                for element in root.find('links'):
                    if element.get('linkDescription') == 'next page':
                        url = self.domain + element.get('link')
                        break
            else:
                url = None
            for ID in root.findall("account"):
                accountIDX = ID.find("accountId").text
                accountIDs.append(accountIDX)
                print (accountIDX)
        return accountIDs
    
    def get_custom_field(self, customfieldID):
        resource = self.domain + '/dataExchangeSettings/customField/' + customfieldID
        response = requests.get(resource, auth=(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)
        return response

    def get_custom_field_data(self, accountID):
        resource = self.domain + 'account/' + accountID + '/customFieldList' 
        response = requests.get(resource, auth=(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)
        return response
        
    def post_accept_invite(self, template_file, accountID):
        if hasattr(template_file, "read"):
            resource = self.domain + '/connect/account/' + accountID
            template_info = template_file.read()
            headers = {'Content-Type': 'application/xml'}
            acct = str(template_info)
            response = requests.post(resource, data=acct, auth=(self.username, self.password), headers=headers)
            print(response.text)
            if response.status_code != 200:
                return _raise_for_status(response)
            return response.text
        
    def post_disconnect(self, template_file, accountID):
        if hasattr(template_file, "read"):
            resource = self.domain + '/disconnect/account/' + accountID
            template_info = template_file.read()
            headers = {'Content-Type': 'application/xml'}
            acct = str(template_info)
            response = requests.post(resource, data=acct, auth=(self.username, self.password), headers=headers)
            if response.status_code != 200:
                return _raise_for_status(response)
            return response.text 

    def get_customer_list(self):
        resource = self.domain + 'customer/list' 
        response = requests.get(resource, auth=(self.username, self.password))
        accountIDs = []
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('links'):
            for child2 in child.findall('link'):
                accountIDY = child2.get('id')
                accountIDs.append(accountIDY)

        if response.status_code != 200:
            return _raise_for_status(response)
        return accountIDs
    def get_customer_info(self,accountID):
        resource = self.domain + 'customer/' + accountID 
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        if response.status_code != 200:
            return _raise_for_status(response)
        return response

### Property Connection Calls
    def get_pending_propertyconnection_list_multipage_accept(self, DCRealPropertyIDList):
        resource = self.domain + "/share/property/pending/list"
        url = resource + "?page=1"
        page = 1
        PropertyIDs = []
        DCRealPropertyID = []
        while url:
            print (url)
            print("Getting data from page " + str(page))
            page += 1
            response = requests.get(url, auth=(self.username, self.password))
            if response.status_code != 200:
                print(response.status_code, response.reason)
                break
            dataforlink = response.text
            root = Et.fromstring(dataforlink)
            if root.find('links') is not None:
                for element in root.find('links'):
                    if element.get('linkDescription') == 'next page':
                        url = self.domain + element.get('link')
                        break
            else:
                url = None
            for ID in root.findall("property"):
                PropertyIDY = ID.find("propertyId").text
                for DC in root.findall("property"):
                    for DC2 in DC.findall(".//*[@name='DC Real Property ID']"):
                        DCRealPropertyIDY = DC2.text
                if DCRealPropertyIDY in DCRealPropertyIDList:
                        PropertyIDs.append(PropertyIDY)

        return PropertyIDs
    def get_pending_propertyconnection_list_multipage_reject(self, DCRealPropertyIDList):
        resource = self.domain + "/share/property/pending/list"
        url = resource + "?page=1"
        page = 1
        PropertyIDs = []
        DCRealPropertyID = []
        while url:
            print (url)
            print("Getting data from page " + str(page))
            page += 1
            response = requests.get(url, auth=(self.username, self.password))
            if response.status_code != 200:
                print(response.status_code, response.reason)
                break
            dataforlink = response.text
            root = Et.fromstring(dataforlink)
            if root.find('links') is not None:
                for element in root.find('links'):
                    if element.get('linkDescription') == 'next page':
                        url = self.domain + element.get('link')
                        break
            else:
                url = None
            for ID in root.findall("property"):
                PropertyIDY = ID.find("propertyId").text
                for DC in root.findall("property"):
                    for DC2 in DC.findall(".//*[@name='DC Real Property ID']"):
                        DCRealPropertyIDY = DC2.text
                if DCRealPropertyIDY not in DCRealPropertyIDList:
                        PropertyIDs.append(PropertyIDY)

        return PropertyIDs
        
    def get_pending_meterconnection_list_multipage_accept(self,propertyIDstoAccept):
        resource = self.domain + "/share/meter/pending/list"
        url = resource + "?page=1"
        page = 1
        meterIDs = []
        while url:
            print (url)
            print("Getting data from page " + str(page))
            page += 1
            response = requests.get(url, auth=(self.username, self.password))
            if response.status_code != 200:
                print(response.status_code, response.reason)
                break
            dataforlink = response.text
            root = Et.fromstring(dataforlink)
            if root.find('links') is not None:
                for element in root.find('links'):
                    if element.get('linkDescription') == 'next page':
                        url = self.domain + element.get('link')
                        break
            else:
                url = None
            for ID in root.findall("meter"):
                meterIDY = ID.find("meterId").text
                propertyIDY = ID.find("propertyId").text
                if propertyIDY in propertyIDstoAccept:
                    meterIDs.append(meterIDY)
        return meterIDs
    def get_pending_meterconnection_list_multipage_reject(self,propertyIDstoReject):
        resource = self.domain + "/share/meter/pending/list"
        url = resource + "?page=1"
        page = 1
        meterIDs = []
        while url:
            print (url)
            print("Getting data from page " + str(page))
            page += 1
            response = requests.get(url, auth=(self.username, self.password))
            if response.status_code != 200:
                print(response.status_code, response.reason)
                break
            dataforlink = response.text
            root = Et.fromstring(dataforlink)
            if root.find('links') is not None:
                for element in root.find('links'):
                    if element.get('linkDescription') == 'next page':
                        url = self.domain + element.get('link')
                        break
            else:
                url = None
            for ID in root.findall("meter"):
                meterIDY = ID.find("meterId").text
                propertyIDY = ID.find("propertyId").text
                if propertyIDY in propertyIDstoReject:
                    meterIDs.append(meterIDY)
        return meterIDs

    def post_meter_response(self, template_file, meterID):
        if hasattr(template_file, "read"):
            resource = self.domain + '/share/meter/' + meterID
            template_info = template_file.read()
            headers = {'Content-Type': 'application/xml'}
            acct = str(template_info)
            response = requests.post(resource, data=acct, auth=(self.username, self.password), headers=headers)
            if response.status_code != 200:
                return _raise_for_status(response)
            return response.text

    def post_property_response(self, template_file, PropertyID):
        if hasattr(template_file, "read"):
            resource = self.domain + '/share/property/' + PropertyID
            template_info = template_file.read()
            headers = {'Content-Type': 'application/xml'}
            acct = str(template_info)
            response = requests.post(resource, data=acct, auth=(self.username, self.password), headers=headers)
            if response.status_code != 200:
                return _raise_for_status(response)
            return response.text

    def get_property_list(self, accountID):
        resource = self.domain + 'account/'+accountID+'/property/list' 
        response = requests.get(resource, auth=(self.username, self.password))
        propertyIDs = []
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('links'):
            for child2 in child.findall('link'):
                propertyIDY = child2.get('id')
                propertyIDs.append(propertyIDY)

        if response.status_code != 200:
            return _raise_for_status(response)
        return propertyIDs
    
    def get_DCRealID(self, propertyID):
        resource = self.domain + '/property/' + propertyID  + '/customFieldList'
        response = requests.get(resource, auth=(self.username, self.password))
        DCIDs = []
        data = response.text
        root = Et.fromstring(data)
        for DC2 in root.findall(".//*[@name='DC Real Property ID']"):
            DCRealPropertyIDY = DC2.text
            DCIDs.append(DCRealPropertyIDY)

        if response.status_code != 200:
            return _raise_for_status(response)
        return DCIDs
    def get_Additional_DCRealID(self, propertyID):
        resource = self.domain + '/property/' + propertyID  + '/customFieldList'
        response = requests.get(resource, auth=(self.username, self.password))
        DCIDs = []
        data = response.text
        root = Et.fromstring(data)
        for DC2 in root.findall(".//*[@name='Additional DC Real Property ID']"):
            DCRealPropertyIDY = DC2.text
            DCIDs.append(DCRealPropertyIDY)

        if response.status_code != 200:
            return _raise_for_status(response)
        return DCIDs    
    def get_Property_ContactName(self, propertyID):
        resource = self.domain + '/property/' + propertyID  + '/customFieldList'
        response = requests.get(resource, auth=(self.username, self.password))
        contactName = []
        data = response.text
        root = Et.fromstring(data)
        for Child in root.findall(".//*[@name='Property Contact Name']"):
            temp = Child.text
            contactName.append(temp)

        if response.status_code != 200:
            return _raise_for_status(response)
        return contactName

    def get_Property_ContactEmail(self, propertyID):
        resource = self.domain + '/property/' + propertyID  + '/customFieldList'
        response = requests.get(resource, auth=(self.username, self.password))
        contactEmail = []
        data = response.text
        root = Et.fromstring(data)
        for Child in root.findall(".//*[@name='Property Contact Email']"):
            temp = Child.text
            contactEmail.append(temp)

        if response.status_code != 200:
            return _raise_for_status(response)
        return contactEmail
    
    def get_property_info(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)
        return response
### Metric Calls 
    def get_metrics(self, PropertyID, year, month, measurements, metricsPM):
        resource = self.domain + '/property/' + PropertyID +'/metrics?year=' + year +'&month=' + month + '&measurementSystem=' + measurements
        headers = {'Content-Type': 'application/xml', 'PM-Metrics': metricsPM}
        response = requests.get(resource, auth=(self.username, self.password), headers = headers)
        if response.status_code != 200:
            return _raise_for_status(response)
        return response

    def get_montly_metrics(self, PropertyID, year, month, measurements, metricsPM):
        resource = self.domain + '/property/' + PropertyID +'/metrics/monthly?year=' + year +'&month=' + month + '&measurementSystem=' + measurements
        headers = {'Content-Type': 'application/xml', 'PM-Metrics': metricsPM}
        response = requests.get(resource, auth=(self.username, self.password), headers = headers)
        if response.status_code != 200:
            return _raise_for_status(response)
        return response

### Unused calls        
    def get_meter_type(self, meter_id):
        resource = self.domain + '/meter/%s' % str(meter_id)

        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text

        root = Et.fromstring(data)
        meter_type = ''
        for e in root.iter("*"):
            if e.tag == "type":
                meter_type = e.text

        return meter_type
             
    def get_building_info(self, prop_id):
        resource = self.domain + '/building/%s' % str(prop_id)
        response = requests.get(resource, auth=(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)
        return response.text

    def get_usage_data(self, meter_id, months_ago):
        # Get date in YYYY-MM-DD format from months ago
        date_format = '%Y-%m-%d'
        today = datetime.datetime.now()
        start_date = today - datetime.timedelta(days=months_ago * 30)
        start_date = datetime.datetime.strftime(start_date, date_format)
        resource = self.domain + '/meter/%s/consumptionData' % str(meter_id)

        usage = []

        url = resource + '?page=1&startDate=' + start_date
        page = 1
        while url:
            print(url)
            print("Getting data from page " + str(page))
            page += 1

            response = requests.get(url, auth=(self.username, self.password))

            if response.status_code != 200:
                print(response.status_code, response.reason)
                break
            # Set URL to none to stop loop if no more links
            url = None

            data = response.text
            root = Et.fromstring(data)
            for element in root.findall("meterConsumption"):
                month_data = dict()
                # Get the usage data
                month_data[element.find("endDate").text] = float(element.find("usage").text)
                usage.append(month_data)

            # Get the next URL
            for element in root.find("links"):
                for link in element.findall("link"):
                    if link.get("linkDescription") == "next page":
                        url = self.domain + link.get("link")
        # Return the usage for the time period
        return usage

    def get_meter_list(self, prop_id):
        resource = self.domain + '/association/property/%s/meter' % str(prop_id)
        response = requests.get(resource, auth=(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)

        data = response.text
        meters = dict()

        root = Et.fromstring(data)

        for e in root.iter("*"):
            if e.tag == "meterId":
                meter_type = self.get_meter_type(e.text)
                meters[e.text] = meter_type

        return meters
        
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
