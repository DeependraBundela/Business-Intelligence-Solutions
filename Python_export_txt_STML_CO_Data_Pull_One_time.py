#!/usr/bin/env python
import snowflake.connector
import os
import sqlite3
import csv

# Setting your account information
ACCOUNT = 'directmailers'
USER = 'username'
PASSWORD = 'password'

# Connecting to the Snowflake DB
cnx = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
)

# Using Database, Schema and Warehouse
cnx.cursor().execute("USE warehouse APP01_WH;")
cnx.cursor().execute("USE database FREE_Reporting;")

################################################################################################################################
##                              End of STML, Start CO                                                                        ###
################################################################################################################################

####################### CO_2017_01_02 ###################################################
print(" ########################################## ")
print(" Data Extraction for CO_2017_01_02_Portfolio - STARTED  ")
Process_Log_status_Message=  "\n Data Extraction for CO_2017_01_02_Portfolio - STARTED  "

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_CO_2017_01_02_Portfolio.txt', 'w')
outcsv = csv.writer(outfile, lineterminator='\n')

cur = cnx.cursor()
cur.execute("""
select DateMailed,DropNumber,campaign,Postcard,Preferred_Description,Offer,ResNum,Response,to_date(ResponseDate) as ResponseDate,Application60,to_date(Applicationdate) as Applicationdate,
Gate60,to_date(INITIAL_DISCLOSURE_COMPLETE) as GateDate,Fund60,CASE WHEN FUND60=1 THEN to_date(CLosingDate)  ELSE NULL END as FundDate, State,City,Amg08plus,Fico,ltv,RecordType,MatchType,LoanAmount,ActualDateMailed,Age,Income,IncomeRange,Maritalstat,InterestRate,EQUITY03,EQUITY06,EQUITY14,EQUITY08,EQUITY10,EQUITY13,DwelType,gender,Zip, 
CASE
WHEN (IMBSERVICETYPE='040' or IMBSERVICETYPE='041') THEN 'First Class'
WHEN (IMBSERVICETYPE='042' or IMBSERVICETYPE='043') THEN 'Third Class' 
WHEN Offer LIKE 'FD.%' THEN  'Third Class' 
ELSE NULL
 END as MailType,FIRSTNAME,LASTNAME from DBO.Freedom_TBE_Current where  STML_FULLDOC='FULLDOC' and Offer LIKE 'FD[_]%%' and (Datemailed like '2017-01%%' or Datemailed like '2017-02%%') ;""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for CO_2017_01_02_Portfolio - DONE  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2017_01_02_Portfolio - DONE  "

#############################################################################################

######################## CO_2016_01_12 ################################################
print(" ########################################## ")
print(" Data Extraction for CO_2016_01_12_Portfolio - STARTED  ")
Process_Log_status_Message= " Data Extraction for CO_2016_01_12_Portfolio - STARTED  "

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_CO_2016_01_12_Portfolio.txt', 'w')
outcsv = csv.writer(outfile, lineterminator='\n')

cur = cnx.cursor()
cur.execute("""

select DateMailed,DropNumber,campaign,Postcard,Preferred_Description,Offer,ResNum,Response,to_date(ResponseDate) as ResponseDate,Application60,to_date(Applicationdate) as Applicationdate,
Gate60,to_date(INITIAL_DISCLOSURE_COMPLETE) as GateDate,Fund60,CASE WHEN FUND60=1 THEN to_date(CLosingDate)  ELSE NULL END as FundDate, State,City,Amg08plus,Fico,ltv,RecordType,MatchType,LoanAmount,ActualDateMailed,Age,Income,IncomeRange,Maritalstat,InterestRate,EQUITY03,EQUITY06,EQUITY14,EQUITY08,EQUITY10,EQUITY13,DwelType,gender,Zip, 
CASE
WHEN (IMBSERVICETYPE='040' or IMBSERVICETYPE='041') THEN 'First Class'
WHEN (IMBSERVICETYPE='042' or IMBSERVICETYPE='043') THEN 'Third Class' 
WHEN Offer LIKE 'FD.%' THEN  'Third Class' 
ELSE NULL
 END as MailType,FIRSTNAME,LASTNAME from DBO.Freedom_TBE_Current where  STML_FULLDOC='FULLDOC' and Offer LIKE 'FD[_]%%' and (Datemailed like '2016-01%%' or Datemailed like '2016-02%%' or Datemailed like '2016-03%%' or Datemailed like '2016-04%%' or Datemailed like '2016-05%%' or Datemailed like '2016-06%%' or Datemailed like '2016-07%%' or Datemailed like '2016-08%%' or Datemailed like '2016-09%%' or Datemailed like '2016-10%%' or Datemailed like '2016-11%%' or  Datemailed like '2016-12%%');""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for CO_2016_01_12_Portfolio - DONE  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2016_01_12_Portfolio - DONE  "

print(Process_Log_status_Message)
# Close File
outfile.close()
