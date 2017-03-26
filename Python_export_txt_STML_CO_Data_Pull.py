#####   STML Pull data #######

#!/usr/bin/env python
import snowflake.connector
import os
import sqlite3
import csv

# Setting your account information
ACCOUNT = 'directmailers'
USER = 'Username'
PASSWORD = 'Password'

# Connecting to the Snowflake DB
cnx = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
)

# Using Database, Schema and Warehouse
cnx.cursor().execute("USE warehouse APP01_WH;")
cnx.cursor().execute("USE database FREE_Reporting;")

######################## STML_2016_07_08 ################################################
print(" ########################################## ")
print(" Data Extraction for STML_2016_07_08 - STARTED  ")
Process_Log_status_Message= " Data Extraction for STML_2016_07_08 - STARTED  "

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_STML_2016_07_08.txt', 'w')
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
 END as MailType,FIRSTNAME,LASTNAME,CUSTOMERID from DBO.Freedom_TBE_Current where  STML_FULLDOC='STML' and Offer NOT LIKE 'FD[_]%%' and (Datemailed like '2016-07%%' or Datemailed like '2016-08%%');""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for STML_2016_07_08 - DONE  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for STML_2016_07_08 - DONE  "

####################### STML_2016_09_10 ###################################################
print(" ########################################## ")
print(" Data Extraction for STML_2016_09_10 - STARTED  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for STML_2016_09_10 - STARTED  "

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_STML_2016_09_10.txt', 'w')
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
 END as MailType,FIRSTNAME,LASTNAME,CUSTOMERID from DBO.Freedom_TBE_Current where  STML_FULLDOC='STML' and Offer NOT LIKE 'FD[_]%%' and (Datemailed like '2016-09%%' or Datemailed like '2016-10%%') ;""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for STML_2016_09_10 - DONE  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for STML_2016_09_10 - DONE  "

######################## STML_2016_11_12 ################################################
print(" ########################################## ")
print(" Data Extraction for STML_2016_11_12 - STARTED  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for STML_2016_11_12 - STARTED  "

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_STML_2016_11_12.txt', 'w')
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
 END as MailType,FIRSTNAME,LASTNAME,CUSTOMERID from DBO.Freedom_TBE_Current where  STML_FULLDOC='STML' and Offer NOT LIKE 'FD[_]%%' and (Datemailed like '2016-11%%' or Datemailed like '2016-12%%');""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for STML_2016_11_12 - DONE  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for STML_2016_11_12 - DONE  "

####################### STML_2017_01 ###################################################
print(" ########################################## ")
print(" Data Extraction for STML_2017_01_02 - STARTED  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for STML_2017_01_02 - STARTED  "

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_STML_2017_01_02.txt', 'w')
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
 END as MailType,FIRSTNAME,LASTNAME,CUSTOMERID from DBO.Freedom_TBE_Current where  STML_FULLDOC='STML' and Offer NOT LIKE 'FD[_]%%' and (Datemailed like '2017-01%%' or Datemailed like '2017-02%%') ;""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for STML_2017_01_02 - DONE  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for STML_2017_01_02 - DONE  "

#############################################################################################


################################################################################################################################
##                              End of STML, Start CO                                                                        ###
################################################################################################################################


######################## CO_2016_07_08 ################################################
print(" ########################################## ")
print(" Data Extraction for CO_2016_07_08 - STARTED  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2016_07_08 - STARTED "

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_CO_2016_07_08.txt', 'w')
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
 END as MailType,FIRSTNAME,LASTNAME,CUSTOMERID from DBO.Freedom_TBE_Current where  STML_FULLDOC='FULLDOC' and Offer NOT LIKE 'FD[_]%%' and (Datemailed like '2016-07%%' or Datemailed like '2016-08%%');""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for CO_2016_07_08 - DONE  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2016_07_08 - DONE "

####################### CO_2016_09_10 ###################################################
print(" ########################################## ")
print(" Data Extraction for CO_2016_09_10 - STARTED  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2016_09_10 - STARTED "

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_CO_2016_09_10.txt', 'w')
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
 END as MailType,FIRSTNAME,LASTNAME,CUSTOMERID from DBO.Freedom_TBE_Current where  STML_FULLDOC='FULLDOC' and Offer NOT LIKE 'FD[_]%%' and (Datemailed like '2016-09%%' or Datemailed like '2016-10%%');""")


# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for CO_2016_09_10 - DONE ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2016_09_10 - DONE "

######################## CO_2016_11_12 ################################################
print(" ########################################## ")
print(" Data Extraction for CO_2016_11_12 - STARTED  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2016_11_12 - STARTED "

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_CO_2016_11_12.txt', 'w')
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
 END as MailType,FIRSTNAME,LASTNAME,CUSTOMERID from DBO.Freedom_TBE_Current where  STML_FULLDOC='FULLDOC' and Offer NOT LIKE 'FD[_]%%' and (Datemailed like '2016-11%%' or Datemailed like '2016-12%%');""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for CO_2016_11_12 - DONE ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2016_11_12 - DONE "

####################### CO_2017_01 ###################################################
print(" ########################################## ")
print(" Data Extraction for CO_2017_01_02 - STARTED  ")

# outfile
outfile = open('C:/Users/Deependra/Som_Data_New/Som_Data_CO_2017_01_02.txt', 'w')
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
 END as MailType,FIRSTNAME,LASTNAME,CUSTOMERID from DBO.Freedom_TBE_Current where  STML_FULLDOC='FULLDOC' and Offer NOT LIKE 'FD[_]%%' and (Datemailed like '2017-01%%' or Datemailed like '2017-02%%');""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for CO_2017_01_02 - DONE  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2017_01_02 - DONE "

##########################################################################
print(Process_Log_status_Message)
# Close File
outfile.close()
