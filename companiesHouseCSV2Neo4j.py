import csv
import sys

from neo4j.v1 import GraphDatabase, basic_auth

with open("BasicCompanyDataAsOneFile-2018-04-01.csv", "r") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        CompanyName                           = row['CompanyName'];
        CompanyNumber                         = row['CompanyNumber'];
        RegAddressCareOf                      = row["RegAddress.CareOf"];
        RegAddressPOBox                       = row["RegAddress.POBox"];
        RegAddressAddressLine1                = row["RegAddress.AddressLine1"];
        RegAddressAddressLine2                = row["RegAddress.AddressLine2"];
        RegAddressPostTown                    = row["RegAddress.PostTown"];
        RegAddressCounty                      = row["RegAddress.County"];
        RegAddressCountry                     = row["RegAddress.Country"];
        RegAddressPostCode                    = row["RegAddress.PostCode"];
        CompanyCategory                       = row["CompanyCategory"];
        CompanyStatus                         = row["CompanyStatus"];
        CountryOfOrigin                       = row["CountryOfOrigin"];
        DissolutionDate                       = row["DissolutionDate"];
        IncorporationDate                     = row["IncorporationDate"];
        AccountsAccountRefDay                 = row["Accounts.AccountRefDay"];
        AccountsAccountRefMonth               = row["Accounts.AccountRefMonth"];
        AccountsNextDueDate                   = row["Accounts.NextDueDate"];
        AccountsLastMadeUpDate                = row["Accounts.LastMadeUpDate"];
        AccountsAccountCategory               = row["Accounts.AccountCategory"];
        ReturnsNextDueDate                    = row["Returns.NextDueDate"];
        ReturnsLastMadeUpDate                 = row["Returns.LastMadeUpDate"];
        MortgagesNumMortCharges               = row["Mortgages.NumMortCharges"];
        MortgagesNumMortOutstanding           = row["Mortgages.NumMortOutstanding"];
        MortgagesNumMortPartSatisfied         = row["Mortgages.NumMortPartSatisfied"];
        MortgagesNumMortSatisfied             = row["Mortgages.NumMortSatisfied"];
        SICCodeSicText_1                      = row["SICCode.SicText_1"];
        SICCodeSicText_2                      = row["SICCode.SicText_2"];
        SICCodeSicText_3                      = row["SICCode.SicText_3"];
        SICCodeSicText_4                      = row["SICCode.SicText_4"];
        LimitedPartnershipsNumGenPartners     = row["LimitedPartnerships.NumGenPartners"];
        LimitedPartnershipsNumLimPartners     = row["LimitedPartnerships.NumLimPartners"];
        URI                                   = row["URI"];
        PreviousName_1CONDATE                 = row["PreviousName_1.CONDATE"];
        PreviousName_1CompanyName             = row["PreviousName_1.CompanyName"];
        PreviousName_2CONDATE                 = row["PreviousName_2.CONDATE"];
        PreviousName_2CompanyName             = row["PreviousName_2.CompanyName"];
        PreviousName_3CONDATE                 = row["PreviousName_3.CONDATE"];
        PreviousName_3CompanyName             = row["PreviousName_3.CompanyName"];
        PreviousName_4CONDATE                 = row["PreviousName_4.CONDATE"];
        PreviousName_4CompanyName             = row["PreviousName_4.CompanyName"];
        ConfStmtNextDueDate                   = row["ConfStmtNextDueDate"];
        ConfStmtLastMadeUpDate                = row["ConfStmtLastMadeUpDate"];

        driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=basic_auth("neo4j", "Bravo-201"))
        session = driver.session()

        session.run("MERGE (:COMPANY { \
          CompanyName:                          {CompanyName}, \
          CompanyNumber:                        {CompanyNumber}, \
          RegAddressCareOf:                     {RegAddressCareOf}, \
          RegAddressPOBox:                      {RegAddressPOBox}, \
          RegAddressAddressLine1:               {RegAddressAddressLine1}, \
          RegAddressAddressLine2:               {RegAddressAddressLine2}, \
          RegAddressPostTown:                   {RegAddressPostTown}, \
          RegAddressCounty:                     {RegAddressCounty}, \
          RegAddressCountry:                    {RegAddressCountry}, \
          RegAddressPostCode:                   {RegAddressPostCode}, \
          CompanyCategory:                      {CompanyCategory}, \
          CompanyStatus:                        {CompanyStatus}, \
          CountryOfOrigin:                      {CountryOfOrigin}, \
          DissolutionDate:                      {DissolutionDate}, \
          IncorporationDate:                    {IncorporationDate}, \
          AccountsAccountRefDay:                {AccountsAccountRefDay}, \
          AccountsAccountRefMonth:              {AccountsAccountRefMonth}, \
          AccountsNextDueDate:                  {AccountsNextDueDate}, \
          AccountsLastMadeUpDate:               {AccountsLastMadeUpDate}, \
          AccountsAccountCategory:              {AccountsAccountCategory}, \
          ReturnsNextDueDate:                   {ReturnsNextDueDate}, \
          ReturnsLastMadeUpDate:                {ReturnsLastMadeUpDate}, \
          MortgagesNumMortCharges:              {MortgagesNumMortCharges}, \
          MortgagesNumMortOutstanding:          {MortgagesNumMortOutstanding}, \
          MortgagesNumMortPartSatisfied:        {MortgagesNumMortPartSatisfied}, \
          MortgagesNumMortSatisfied:            {MortgagesNumMortSatisfied}, \
          SICCodeSicText_1:                     {SICCodeSicText_1}, \
          SICCodeSicText_2:                     {SICCodeSicText_2}, \
          SICCodeSicText_3:                     {SICCodeSicText_3}, \
          SICCodeSicText_4:                     {SICCodeSicText_4}, \
          LimitedPartnershipsNumGenPartners:    {LimitedPartnershipsNumGenPartners}, \
          LimitedPartnershipsNumLimPartners:    {LimitedPartnershipsNumLimPartners}, \
          URI:                                  {URI}, \
          PreviousName_1CONDATE:                {PreviousName_1CONDATE}, \
          PreviousName_1CompanyName:            {PreviousName_1CompanyName}, \
          PreviousName_2CONDATE:                {PreviousName_2CONDATE}, \
          PreviousName_2CompanyName:            {PreviousName_2CompanyName}, \
          PreviousName_3CONDATE:                {PreviousName_3CONDATE}, \
          PreviousName_3CompanyName:            {PreviousName_3CompanyName}, \
          PreviousName_4CONDATE:                {PreviousName_4CONDATE}, \
          PreviousName_4CompanyName:            {PreviousName_4CompanyName}, \
          ConfStmtNextDueDate:                  {ConfStmtNextDueDate}, \
          ConfStmtLastMadeUpDate:               {ConfStmtLastMadeUpDate}})", \
                    {"CompanyName":                         CompanyName, \
                     "CompanyNumber":                       CompanyNumber, \
                     "RegAddressCareOf":                    RegAddressCareOf, \
                     "RegAddressPOBox":                     RegAddressPOBox, \
                     "RegAddressAddressLine1":              RegAddressAddressLine1, \
                     "RegAddressAddressLine2":              RegAddressAddressLine2, \
                     "RegAddressPostTown":                  RegAddressPostTown, \
                     "RegAddressCounty":                    RegAddressCounty, \
                     "RegAddressCountry":                   RegAddressCountry, \
                     "RegAddressPostCode":                  RegAddressPostCode, \
                     "CompanyCategory":                     CompanyCategory, \
                     "CompanyStatus":                       CompanyStatus, \
                     "CountryOfOrigin":                     CountryOfOrigin, \
                     "DissolutionDate":                     DissolutionDate, \
                     "IncorporationDate":                   IncorporationDate, \
                     "AccountsAccountRefDay":               AccountsAccountRefDay, \
                     "AccountsAccountRefMonth":             AccountsAccountRefMonth, \
                     "AccountsNextDueDate":                 AccountsNextDueDate, \
                     "AccountsLastMadeUpDate":              AccountsLastMadeUpDate, \
                     "AccountsAccountCategory":             AccountsAccountCategory, \
                     "ReturnsNextDueDate":                  ReturnsNextDueDate, \
                     "ReturnsLastMadeUpDate":               ReturnsLastMadeUpDate, \
                     "MortgagesNumMortCharges":             MortgagesNumMortCharges, \
                     "MortgagesNumMortOutstanding":         MortgagesNumMortOutstanding, \
                     "MortgagesNumMortPartSatisfied":       MortgagesNumMortPartSatisfied, \
                     "MortgagesNumMortSatisfied":           MortgagesNumMortSatisfied, \
                     "SICCodeSicText_1":                    SICCodeSicText_1, \
                     "SICCodeSicText_2":                    SICCodeSicText_2, \
                     "SICCodeSicText_3":                    SICCodeSicText_3, \
                     "SICCodeSicText_4":                    SICCodeSicText_4, \
                     "LimitedPartnershipsNumGenPartners":   LimitedPartnershipsNumGenPartners, \
                     "LimitedPartnershipsNumLimPartners":   LimitedPartnershipsNumLimPartners, \
                     "URI":                                 URI, \
                     "PreviousName_1CONDATE":               PreviousName_1CONDATE, \
                     "PreviousName_1CompanyName":           PreviousName_1CompanyName, \
                     "PreviousName_2CONDATE":               PreviousName_2CONDATE, \
                     "PreviousName_2CompanyName":           PreviousName_2CompanyName, \
                     "PreviousName_3CONDATE":               PreviousName_3CONDATE, \
                     "PreviousName_3CompanyName":           PreviousName_3CompanyName, \
                     "PreviousName_4CONDATE":               PreviousName_4CONDATE, \
                     "PreviousName_4CompanyName":           PreviousName_4CompanyName, \
                     "ConfStmtNextDueDate":                 ConfStmtNextDueDate, \
                     "ConfStmtLastMadeUpDate":              ConfStmtLastMadeUpDate})
        session.close();
        print(CompanyName)
