import csv
import sys
import json
import sys
import requests
import time
from requests.auth import HTTPBasicAuth
from neo4j.v1 import GraphDatabase, basic_auth

#define API key
APIKEY = "_edIc_SmLWcgfVSDgPk0SLnEZ2VNX1p1_CS2Pl3T"

file = sys.argv[1]
string = sys.argv[2]
with open(file,'rb') as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:	
    if string in row["Property Address"]:
      title = row["Title Number"]
      tenure = row["Tenure"]
      property_addr = row["Property Address"]
      property_district = row["District"]
      property_county = row["County"]
      property_region = row["Region"]
      property_postcode = row["Postcode"]
      owner = row["Proprietor Name (1)"]
      company_num = row["Company Registration No. (1)"]
      company_addr = row["Proprietor (1) Address (1)"]
      try:
      #define arguments
        q = company_num
        search = "https://api.companieshouse.gov.uk/search/companies?q="+q
        r = requests.get(search, auth=HTTPBasicAuth(APIKEY, ""))
        jData = json.loads(r.content)
      except:
        pass

      for data in jData["items"]:
        try:
          global c_name
          global c_number
          global c_next_due
          global c_next_made_up
          global overdue
          company      = "https://api.companieshouse.gov.uk/company/"+data["company_number"]
          officers     = "https://api.companieshouse.gov.uk/company/"+data["company_number"]+"/officers"
          z            = requests.get(company, auth=HTTPBasicAuth(APIKEY, ""))
          r            = requests.get(officers, auth=HTTPBasicAuth(APIKEY, ""))
          companies    = json.loads(z.content)
          officer      = json.loads(r.content) 
          status       = companies["company_status"]
          c            = companies["accounts"]
          c_name       = companies["company_name"]
          c_number     = companies["company_number"]
          next_due     = c["next_due"]
          next_made_up = c["next_made_up_to"]
          overdue      = c["overdue"]
          for o in officer["items"]:
            o_name  = ("name"+" "+o["name"])
            o_role  = ("role"+" "+o["officer_role"])
          print(c_name) 
        except:
          pass

      try:
        driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=basic_auth("neo4j", "Bravo-201"))
        session = driver.session()
        session.run("MERGE (:COMPANY { \
          company_name:       {company_name}, \
          company_number:     {company_number}, \
          status:             {status}, \
          next_Due:           {next_due}, \
          next_made_up:       {next_made_up}, \
          officer_name:       {officer_name}, \
          officer_role:       {officer_role}})", \
            {"company_name":  c_name, \
            "company_number":  c_number, \
            "status":          status, \
            "next_due":        next_due, \
            "next_made_up":    next_made_up, \
            "officer_name":    o_name, \
            "officer_role":    o_role})
      except:
        pass

      try:
        session.run("MERGE (:ESTATE { \
          Title: {Title}, \
          Tenure: {Tenure}, \
          Property_address:  {Property_address}, \
          Property_district: {Property_district}, \
          Property_county:   {Property_county}, \
          Property_region:   {Property_region}, \
          Property_postcode: {Property_postcode}, \
          Company_name:      {Company_name}, \
          Company_number:    {Company_num}, \
          Company_addr:      {Company_addr}})",
            {"Title":	          title,
             "Tenure":              tenure,
             "Property_address":    property_addr, \
             "Property_district":   property_district, \
             "Property_county":     property_county, \
             "Property_region":     property_region, \
             "Property_postcode":   property_postcode, \
             "Company_name":        owner, \
             "Company_num":      company_num, \
             "Company_addr":        company_addr})
        session.close()
      except:
        pass
      time.sleep(1)
