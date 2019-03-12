#####
#
# Energy Star API
#
#####
import requests
import datetime
import array
import xml.etree.ElementTree as Et
from fuzzywuzzy import fuzz, process
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
            dataforlink = response.text
            root = Et.fromstring(dataforlink)
            for ID in root.findall("account"):
                accountIDX = ID.find("accountId").text
                accountIDs.append(accountIDX)
                print (accountIDX)

            if response.status_code != 200:
                print(response.status_code, response.reason)
                break

            if root.find('links') is not None:
                for element in root.find('links'):
                    if element.get('linkDescription') == 'next page':
                        url = self.domain + element.get('link')
                        break
            else:
                url = None

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
        PropertyIDX = []
        DCRealPropertyIDX = []
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
                PropertyIDX.append(ID.find("propertyId").text)
            for DC in root.findall("property"):
                for DC2 in DC.findall(".//*[@name='DC Real Property ID']"):
                    DCRealPropertyIDX.append(DC2.text)


            i = 0
            while i <len(PropertyIDX):
                ratio = process.extractOne(str(DCRealPropertyIDX[i]), DCRealPropertyIDList)
                if ratio[1] >= 85:
                    PropertyIDs.append(PropertyIDX[i])
                    i+=1
                else:
                    i+=1

        return PropertyIDs
    def get_pending_propertyconnection_list_multipage_reject(self, DCRealPropertyIDList):
        resource = self.domain + "/share/property/pending/list"
        url = resource + "?page=1"
        page = 1
        PropertyIDs = []
        DCRealPropertyID = []
        PropertyIDX = []
        DCRealPropertyIDX = []
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
                PropertyIDX.append(ID.find("propertyId").text)
            for DC in root.findall("property"):
                for DC2 in DC.findall(".//*[@name='DC Real Property ID']"):
                    DCRealPropertyIDX.append(DC2.text)


            i = 0
            while i <len(PropertyIDX):
                ratio = process.extractOne(str(DCRealPropertyIDX[i]), DCRealPropertyIDList)
                if ratio[1] < 85:
                    PropertyIDs.append(PropertyIDX[i])
                    i+=1
                else:
                    i+=1

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
    def get_pending_meterconnection_list_multipage_reject(self):
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
        
    def post_property_disconnect(self, template_file, PropertyID):
        if hasattr(template_file, "read"):
            resource = self.domain + '/unshare/property/' + PropertyID
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
        data = response.text
        root = Et.fromstring(data)
        for DC2 in root.findall(".//*[@name='DC Real Property ID']"):
            DCRealPropertyIDY = DC2.text
            DCRealPropertyIDY = str(DCRealPropertyIDY).zfill(8)

        if response.status_code != 200:
            return _raise_for_status(response)
        return DCRealPropertyIDY
    
    def get_Additional_DCRealID(self, propertyID):
        resource = self.domain + '/property/' + propertyID  + '/customFieldList'
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for DC2 in root.findall(".//*[@name='Additional DC Real Property ID']"):
            DCRealPropertyIDY = DC2.text
        if DCRealPropertyIDY is None:
            DCRealPropertyIDY = ""
        if response.status_code != 200:
            return _raise_for_status(response)
        return DCRealPropertyIDY
    
    def get_Property_ContactName(self, propertyID):
        resource = self.domain + '/property/' + propertyID  + '/customFieldList'
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for Child in root.findall(".//*[@name='Property Contact Name']"):
            contactName = Child.text

        if response.status_code != 200:
            return _raise_for_status(response)
        return contactName

    def get_Property_ContactEmail(self, propertyID):
        resource = self.domain + '/property/' + propertyID  + '/customFieldList'
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for Child in root.findall(".//*[@name='Property Contact Email']"):
            contactEmail = Child.text

        if response.status_code != 200:
            return _raise_for_status(response)
        return contactEmail
    
    def get_property_info(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)
        return response
    
    def get_property_name(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        property_Name =  root.find('name')

        if response.status_code != 200:
            return _raise_for_status(response)
        return property_Name.text
    
    def get_property_address1(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('address'):
            address1 = child.get('address1')

        if response.status_code != 200:
            return _raise_for_status(response)
        return address1
    
    def get_property_address2(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('address'):
            address2 = child.get('address2')
        if address2 is None:
            address2 = " "
        if response.status_code != 200:
            return _raise_for_status(response)
        return address2
    
    def get_property_city(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('address'):
            city = child.get('city')

        if response.status_code != 200:
            return _raise_for_status(response)
        return city
    
    def get_property_postal_code(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('address'):
            postal_Code = child.get('postalCode')

        if response.status_code != 200:
            return _raise_for_status(response)
        return postal_Code
    
    def get_property_state(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('address'):
            state = child.get('state')

        if response.status_code != 200:
            return _raise_for_status(response)
        return state
    
    def get_property_county(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('address'):
            county = child.get('county')
        if county is None:
            county = " "
        if response.status_code != 200:
            return _raise_for_status(response)
        return county
    
    def get_property_country(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('address'):
            country = child.get('country')

        if response.status_code != 200:
            return _raise_for_status(response)
        return country
    
    def get_property_type_self_selected(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        primary_Function =  root.find('primaryFunction')

        if response.status_code != 200:
            return _raise_for_status(response)
        return primary_Function.text
    
    def get_property_construction_status(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        construction_Status =  root.find('constructionStatus')

        if response.status_code != 200:
            return _raise_for_status(response)
        return construction_Status.text
    
    def get_property_number_of_buildings(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        number_Of_Buildings =  root.find('numberOfBuildings')

        if response.status_code != 200:
            return _raise_for_status(response)
        return number_Of_Buildings.text
    
    def get_property_year_built(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        year_Built =  root.find('yearBuilt')

        if response.status_code != 200:
            return _raise_for_status(response)
        return year_Built.text
    
    def get_property_occupancy(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        occupancy_Percentage =  root.find('occupancyPercentage')

        if response.status_code != 200:
            return _raise_for_status(response)
        return occupancy_Percentage.text
    
    def get_property_notes(self, propertyID):
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        notes =  root.find('notes')
        if notes is None:
            notes = ''
        else:
            notes = notes.text

        if response.status_code != 200:
            return _raise_for_status(response)
        return notes
    def get_property_irrigated_area(self, propertyID):
        irrigated_Area = None
        resource = self.domain + '/property/' + propertyID
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text
        root = Et.fromstring(data)
        for child in root.findall('irrigatedArea'):
            irrigated_Area = child.find('value')
        if irrigated_Area is None:
            irrigated_Area = ''
        else:
            irrigated_Area = irrigated_Area.text

        if response.status_code != 200:
            return _raise_for_status(response)
        return irrigated_Area
    
    def get_property_identifiers_list(self, propertyID):
        resource = self.domain + '/property/'+propertyID+'/identifier/list'
        response = requests.get(resource, auth=(self.username, self.password))
        data = response.text

        if response.status_code != 200:
            return _raise_for_status(response)
        return data
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
### Meter Calls
    def get_meter_association(self, PropertyID):
        resource = self.domain + '/association/property/' + PropertyID + '/meter'
        response = requests.get(resource, auth=(self.username, self.password))

        if response.status_code != 200:
            return _raise_for_status(response)
        return response.text    
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
