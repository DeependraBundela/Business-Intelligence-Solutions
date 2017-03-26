#!/usr/bin/env python
import snowflake.connector
import os
import sqlite3
import csv

# Setting your account information
ACCOUNT = 'directmailers'
USER = 'Deependra'
PASSWORD = 'T@ble@u2048'

# Connecting to the Snowflake DB
cnx = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
)

# Using Database, Schema and Warehouse
cnx.cursor().execute("USE warehouse APP01_WH;")
cnx.cursor().execute("USE database FREE_Reporting;")





####################### CO_2017_01_02 ###################################################
print(" ########################################## ")
print(" Data Extraction for CO_2017_01_02_Portfolio - STARTED  ")
Process_Log_status_Message=  "\n Data Extraction for CO_2017_01_02_Portfolio - STARTED  "

# outfile
outfile = open('C:\\Deep\\Python\\Project_Som_Data\\QC_Test.txt', 'w')
outcsv = csv.writer(outfile, lineterminator='\n')

cur = cnx.cursor()
cur.execute("""
SELECT Count(*) FROM dbo.Som_Data_Reports_STML ;""")

# dump rows
outcsv.writerow([col[0] for col in cur.description])
outcsv.writerows(cur.fetchall())
print(" Data Extraction for CO_2017_01_02_Portfolio - DONE  ")
Process_Log_status_Message= Process_Log_status_Message + "\n Data Extraction for CO_2017_01_02_Portfolio - DONE  "

#############################################################################################


outfile.close()
