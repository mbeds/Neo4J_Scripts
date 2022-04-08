import csv
import sys

from neo4j.v1 import GraphDatabase, basic_auth

with open("CCOD_FULL_2018_04.csv", "r") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        Title                                 = row['Title Number'];
        Tenure                                = row['Tenure'];
        PropAddr                              = row["Property Address"];
        District                              = row["District"];
        County                                = row["County"];
        Region                                = row["Region"];
        Postcode                              = row["Postcode"];
        Multiple_Address_Indicator            = row["Multiple Address Indicator"];
        Price_Paid                            = row["Price Paid"];
        Proprietor_Name                       = row["Proprietor Name (1)"];
        Company_Registration_No               = row["Company Registration No. (1)"];

        driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=basic_auth("neo4j", "Bravo-201"))
        session = driver.session()

        session.run("MERGE (:LAND { \
          Title:                          {Title}, \
          Tenure:                         {Tenure}, \
          PropAddr:                       {PropAddr}, \
          District:                       {District}, \
          County:                         {County}, \
          Region:                         {Region}, \
          Postcode:                       {Postcode}, \
          Multiple_Address_Indicator:     {Multiple_Address_Indicator}, \
          Price_Paid:                     {Price_Paid}, \
          Proprietor_Name:                {Proprietor_Name}, \
          Company_Registration_No:        {Company_Registration_No}})", \
                    {"Title":                       Title, \
                     "Tenure":                      Tenure, \
                     "PropAddr":                    PropAddr, \
                     "District":                    District, \
                     "County":                      County, \
                     "Region":                      Region, \
                     "Postcode":                    Postcode, \
                     "Multiple_Address_Indicator":  Multiple_Address_Indicator, \
                     "Price_Paid":                  Price_Paid, \
                     "Proprietor_Name":             Proprietor_Name, \
                     "Company_Registration_No":     Company_Registration_No})
        session.close();
        print(Proprietor_Name)

