##### QC Automation ######

#!/usr/bin/env python
import pandas as pd
import numpy as np
import datetime as dt
import snowflake.connector
import os
import sqlite3
import csv
import smtplib
import datetime

############################################  Email Notification Configuration starts  #################################################################################################################################

# Email File Configuration
def SendEmail(text):
    s=smtplib.SMTP("smtp.office365.com", 587)
    s.ehlo()
    s.starttls()
    s.login("email@abc.com", "password")

    from_addr = 'email@abc.com'
    to_addr = 'email@abc.com'

    subj = "Data QC NOTIFICATION"
    date = datetime.datetime.now().strftime( "%d/%m/%Y %H:%M" )

    message_text = text

    msg = "From: %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % ( from_addr, to_addr, subj, date, message_text )

    s.sendmail(from_addr, to_addr, msg)
    s.quit()
    return;

############################################  Email Notification Configuration Ends  #################################################################################################################################

############################################  SnowFlake Connection Configurtion starts  #################################################################################################################################

# Setting your account information
ACCOUNT = 'account'
USER = 'username'
PASSWORD = 'password'

# Connecting to the Snowflake DB
cnx = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
)

############################################  SnowFlake Connection Configurtion Ends  #################################################################################################################################


############################################  Fetching Dat from Snowflake starts  #################################################################################################################################

# Using Database, Schema and Warehouse -OLD Report
cnx.cursor().execute("USE warehouse APP01_WH;")
cnx.cursor().execute("USE database FREE_Reporting;")


cur_Report_ID = cnx.cursor().execute(""" select max(Report_Id)FROM dbo.Som_Data_Reports_STML; """).fetchone()
Max_Report_Id = cur_Report_ID[0]

print(Max_Report_Id)

cur = cnx.cursor()
cur.execute(""" SELECT * FROM dbo.Som_Data_Reports_STML where Report_Id = %s ;  """, Max_Report_Id )
old=pd.DataFrame(cur.fetchall())
old.columns = [col[0] for col in cur.description]

# Using Database, Schema and Warehouse -new Report
cnx.cursor().execute("USE warehouse APP01_WH;")
cnx.cursor().execute("USE database FREE_Reporting;")
cur = cnx.cursor()
cur.execute(""" SELECT DATEMAILED, COUNT(1) as Mailed, sum(response) as RESPONSE, sum(Application60)as APP, sum(Gate60) as GATE, sum(Fund60) as FUND 
From 
DBO.Freedom_TBE_Current where STML_FULLDOC='STML' and Offer NOT LIKE 'FD[_]%%' and (datemailed like ('2016-07%%') or datemailed like ('2016-08%%') or datemailed like ('2016-09%%') or datemailed like ('2016-10%%') or datemailed like ('2016-11%%') or datemailed like ('2016-12%%') or datemailed like ('2017-015%') or datemailed like ('2017-02%%') )  group by datemailed order by datemailed;  """)
new=pd.DataFrame(cur.fetchall())
new.columns = [col[0] for col in cur.description]

############################################  Fetching Dat from Snowflake Ends  #################################################################################################################################

############################################  Building Difference Report Starts  #################################################################################################################################
Date_NEW = new['DATEMAILED']
#print(Date_NEW)
Date_Old = old['DATEMAILED']
#print(Date_Old)
#new['Response'] = (new['Response'].str.split()).replace(',', '')

Old_record = pd.DataFrame([['','','','','','','','','','','','','','','','','','','']],columns=['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff'])

for i in Date_NEW:
  
     if  str(i) in set(old['DATEMAILED']):
          
          Report_Date_old=old.loc[old.DATEMAILED == str(i), 'REPORT_DATE']
          Report_Date_old=(Report_Date_old.astype(str)).iloc[0]

          
          Mailed_new=new.loc[new.DATEMAILED == i, 'MAILED']
          Mailed_old=old.loc[old.DATEMAILED == str(i), 'MAILED']
          Mailed_diff = (Mailed_new.astype(int)-Mailed_old.astype(int)).iloc[0]
          Mailed_new=(Mailed_new.astype(int)).iloc[0]
          Mailed_old=(Mailed_old.astype(int)).iloc[0]
          
          Response_new=new.loc[new.DATEMAILED == i, 'RESPONSE']
          Response_old=old.loc[old.DATEMAILED == str(i), 'RESPONSE']
          Response_diff = (Response_new.astype(int)-Response_old.astype(int)).iloc[0]
          Response_new=(Response_new.astype(int) - (Response_new.astype(int) - Response_new.astype(int))).iloc[0]
          Response_old=(Response_old.astype(int)).iloc[0]
          

          App_new=new.loc[new.DATEMAILED == i, 'APP']
          App_old=old.loc[old.DATEMAILED == str(i), 'APP']
          App_diff = (App_new.astype(int)-App_old.astype(int)).iloc[0]
          App_new=(App_new.astype(int)).iloc[0]
          App_old=(App_old.astype(int)).iloc[0]
          
          Gate_new=new.loc[new.DATEMAILED == i, 'GATE']
          Gate_old=old.loc[old.DATEMAILED == str(i), 'GATE']
          Gate_diff = (Gate_new.astype(int)-Gate_old.astype(int)).iloc[0]
          Gate_new=(Gate_new.astype(int)).iloc[0]
          Gate_old=(Gate_old.astype(int)).iloc[0]

          Fund_new=new.loc[new.DATEMAILED == i, 'FUND']
          Fund_old=old.loc[old.DATEMAILED == str(i), 'FUND']
          Fund_diff = (Fund_new.astype(int)-Fund_old.astype(int)).iloc[0]
          Fund_new=(Fund_new.astype(int)).iloc[0]
          Fund_old=(Fund_old.astype(int)).iloc[0]
          
          New_record = pd.DataFrame([[i,Mailed_new,Response_new,App_new,Gate_new,Fund_new,'',Report_Date_old,i,Mailed_old,Response_old,App_old,Gate_old,Fund_old,Mailed_diff,Response_diff,App_diff,Gate_diff,Fund_diff]],columns=['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff'])

          Old_record = pd.concat([Old_record,New_record])
     else:

          Mailed_new=new.loc[new.DATEMAILED == i, 'MAILED']
          Mailed_new=(Mailed_new.astype(int)).iloc[0]

          Response_new=new.loc[new.DATEMAILED == i, 'RESPONSE']
          Response_new=(Response_new.astype(int) - (Response_new.astype(int) - Response_new.astype(int))).iloc[0]

          App_new=new.loc[new.DATEMAILED == i, 'APP']
          App_new=(App_new.astype(int)).iloc[0]

          Gate_new=new.loc[new.DATEMAILED == i, 'GATE']
          Gate_new=(Gate_new.astype(int)).iloc[0]

          Fund_new=new.loc[new.DATEMAILED == i, 'FUND']
          Fund_new=(Fund_new.astype(int)).iloc[0]
          
          New_record = pd.DataFrame([[i,Mailed_new,Response_new,App_new,Gate_new,Fund_new,'','','','','','','','','','','','','']],columns=['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff'])
          Old_record = pd.concat([Old_record,New_record]) 

Old_record = pd.concat([Old_record,New_record])

############################################  Building Difference Report Ends  #################################################################################################################################

#################################################   Saving Sum Row to DataFrame   ############################################################################################################################
Qc_Old_record = Old_record.replace(r'\s*', np.nan, regex=True)
Qc_Old_record.fillna(0, inplace=True)

####   Adding sum row to the datfarme ######

sum_record = pd.DataFrame([["Totals",str(Qc_Old_record['Mailed_New'].sum().astype(int)),str(Qc_Old_record['Response_New'].sum().astype(int)),str(Qc_Old_record['App_New'].sum().astype(int)),str(Qc_Old_record['Gate_New'].sum().astype(int)),str(Qc_Old_record['Fund_New'].sum().astype(int)),'','','',str(Qc_Old_record['Mailed_Old'].sum().astype(int)),
str(Qc_Old_record['Response_Old'].sum().astype(int)),str(Qc_Old_record['App_Old'].sum().astype(int)),str(Qc_Old_record['Gate_Old'].sum().astype(int)),str(Qc_Old_record['Fund_Old'].sum().astype(int)),
str(Qc_Old_record['Mailed_Diff'].sum().astype(int)),str(Qc_Old_record['Response_Diff'].sum().astype(int)),str(Qc_Old_record['App_Diff'].sum().astype(int)),
str(Qc_Old_record['Gate_Diff'].sum().astype(int)),str(Qc_Old_record['Fund_Diff'].sum().astype(int))]],columns=['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff'])

Old_record_save = pd.concat([Old_record,sum_record])

#Old_record_save.to_csv('C:\\Deep\\Python\\Project_Som_Data\\QC_Result\\Som_Data_QC_Result_STML.csv',mode='w')

######################################################################## QC Checks starts Here ###############################################################################

QC_Check_Counter=0
QC_Check_Error_Message = ""


### Checking the Difference in Mailed Quanity
for i in range(0,Qc_Old_record['Mailed_Diff'].count()):
     if Qc_Old_record['Mailed_Diff'].iloc[i] > 500:
          QC_Check_Counter = 1
          QC_Check_Error_Message = QC_Check_Error_Message + "\n Mail difference for " + str(Qc_Old_record['DATEMAILED_New'].iloc[i])+ " is greater than expected." + " Mail difference is " + str(Qc_Old_record['Mailed_Diff'].iloc[i])

### Checking the Difference in Fund Quanity
for i in range(0,Qc_Old_record['Fund_Diff'].count()):
     if Qc_Old_record['Fund_Diff'].iloc[i] > 50:
          QC_Check_Counter = 1
          QC_Check_Error_Message = QC_Check_Error_Message + "\n Fund difference for " + str(Qc_Old_record['DATEMAILED_New'].iloc[i])+ " is greater than expected." + " Fund difference is " + str(Qc_Old_record['Fund_Diff'].iloc[i])
        

### CHECKING NEW MAILED, RESPONSE, APP, GATE, FUND COUNTS ARE HIGHER OR SAME AS COMPARED TO PREVIOUS  
if Qc_Old_record['Mailed_New'].sum() < Qc_Old_record['Mailed_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message = QC_Check_Error_Message + "\n New total Mailed Quantity " + str(Qc_Old_record['Mailed_New'].sum()) + " is less than previous reported count " + str(Qc_Old_record['Mailed_Old'].sum())

if Qc_Old_record['Response_New'].sum() < Qc_Old_record['Response_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message =  QC_Check_Error_Message + "\n New total Response Quantity " + str(Qc_Old_record['Response_New'].sum()) + " is less than previous reported count " + str(Qc_Old_record['Response_Old'].sum())
     
if Qc_Old_record['App_New'].sum() < Qc_Old_record['App_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message =  QC_Check_Error_Message + "\n New total App Quantity " + str(Qc_Old_record['App_New'].sum()) + " is less than previous reported count " + str(Qc_Old_record['App_Old'].sum())

if Qc_Old_record['Gate_New'].sum() < Qc_Old_record['Gate_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message =  QC_Check_Error_Message + "\n New total Gate Quantity " + str(Qc_Old_record['Gate_New'].sum()) + " is less than previous reported count " + str(Qc_Old_record['Gate_Old'].sum())

if Qc_Old_record['Fund_New'].sum() < Qc_Old_record['Fund_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message =  QC_Check_Error_Message + "\n New total Fund Quantity " + str(Qc_Old_record['Fund_New'].sum()) + " is less than previous reported total mailed quanity " + str(Qc_Old_record['Fund_Old'].sum())

Qc_Old_record['Mailed_New'].sum()
Qc_Old_record['Response_New'].sum()
Qc_Old_record['App_New'].sum()
Qc_Old_record['Gate_New'].sum()
Qc_Old_record['Fund_New'].sum()
Qc_Old_record['Mailed_Old'].sum()
Qc_Old_record['Response_Old'].sum()
Qc_Old_record['App_Old'].sum()
Qc_Old_record['Gate_Old'].sum()
Qc_Old_record['Fund_Old'].sum()
Qc_Old_record['Mailed_Diff'].sum()
Qc_Old_record['App_Diff'].sum()
Qc_Old_record['Gate_Diff'].sum()
Qc_Old_record['Fund_Diff'].sum()
          
############################################################################################ Writing report to Excel ###########################################################################################

writer = pd.ExcelWriter("C:\\Users\\Deependra\\Som_Data_New\\Som_Data_QC_Result_STML.xlsx", engine='xlsxwriter')
# Convert the dataframe to an XlsxWriter Excel object.

#Old_record_print=Old_record_save[['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff']]
Old_record_print=Old_record_save.iloc[1:,:]
Old_record_print.to_excel(writer, sheet_name='QC_Report')

# Get the xlsxwriter workbook and worksheet objects.
workbook  = writer.book
worksheet = writer.sheets['QC_Report']

# Add some cell formats.

format3 = workbook.add_format({'bold': True,'align':'center'})

format2 = workbook.add_format({'align':'center'})
#format3.set_border(4)

# Set the column width and format.
worksheet.set_column('K:T', 13, format2)
worksheet.set_column('C:G', 13, format2)
worksheet.set_column('I:J', 16, format2)
worksheet.set_column('B:B', 16, format2)
worksheet.set_row(0,20, format3)
worksheet.set_row(-1,20, format3)

# Close the Pandas Excel writer and output the Excel file.
writer.save()

############################################################################################ End ###########################################################################################

############################################  Cash Out Validation  #################################################################################################################################

# Using Database, Schema and Warehouse -OLD Report
cnx.cursor().execute("USE warehouse APP01_WH;")
cnx.cursor().execute("USE database FREE_Reporting;")

cur_Report_ID = cnx.cursor().execute(""" select max(Report_Id)FROM dbo.Som_Data_Reports_CO; """).fetchone()
Max_Report_Id = cur_Report_ID[0]

print(Max_Report_Id)

cur = cnx.cursor()
cur.execute(""" SELECT * FROM dbo.Som_Data_Reports_CO where Report_Id = %s ;  """, Max_Report_Id )
old=pd.DataFrame(cur.fetchall())
old.columns = [col[0] for col in cur.description]

# Using Database, Schema and Warehouse -new Report
cnx.cursor().execute("USE warehouse APP01_WH;")
cnx.cursor().execute("USE database FREE_Reporting;")
cur = cnx.cursor()
cur.execute(""" SELECT DATEMAILED, COUNT(1) as Mailed, sum(response) as RESPONSE, sum(Application60)as APP, sum(Gate60) as GATE, sum(Fund60) as FUND 
From 
DBO.Freedom_TBE_Current where STML_FULLDOC='FULLDOC' and Offer NOT LIKE 'FD[_]%%' and (datemailed like ('2016-07%%') or datemailed like ('2016-08%%') or datemailed like ('2016-09%%') or datemailed like ('2016-10%%') or datemailed like ('2016-11%%') or datemailed like ('2016-12%%') or datemailed like ('2017-015%') or datemailed like ('2017-02%%') )  group by datemailed order by datemailed;  """)
new=pd.DataFrame(cur.fetchall())
new.columns = [col[0] for col in cur.description]

############################################  Fetching Dat from Snowflake Ends  #################################################################################################################################

############################################  Building Difference Report Starts  #################################################################################################################################
Date_NEW = new['DATEMAILED']
#print(Date_NEW)
Date_Old = old['DATEMAILED']
#print(Date_Old)
#new['Response'] = (new['Response'].str.split()).replace(',', '')

Old_record = pd.DataFrame([['','','','','','','','','','','','','','','','','','','']],columns=['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff'])


for i in Date_NEW:
  
     if  str(i) in set(old['DATEMAILED']):
          
          Report_Date_old=old.loc[old.DATEMAILED == str(i), 'REPORT_DATE']
          Report_Date_old=(Report_Date_old.astype(str)).iloc[0]

          
          Mailed_new=new.loc[new.DATEMAILED == i, 'MAILED']
          Mailed_old=old.loc[old.DATEMAILED == str(i), 'MAILED']
          Mailed_diff = (Mailed_new.astype(int)-Mailed_old.astype(int)).iloc[0]
          Mailed_new=(Mailed_new.astype(int)).iloc[0]
          Mailed_old=(Mailed_old.astype(int)).iloc[0]
          
          Response_new=new.loc[new.DATEMAILED == i, 'RESPONSE']
          Response_old=old.loc[old.DATEMAILED == str(i), 'RESPONSE']
          Response_diff = (Response_new.astype(int)-Response_old.astype(int)).iloc[0]
          Response_new=(Response_new.astype(int) - (Response_new.astype(int) - Response_new.astype(int))).iloc[0]
          Response_old=(Response_old.astype(int)).iloc[0]
          
          App_new=new.loc[new.DATEMAILED == i, 'APP']
          App_old=old.loc[old.DATEMAILED == str(i), 'APP']
          App_diff = (App_new.astype(int)-App_old.astype(int)).iloc[0]
          App_new=(App_new.astype(int)).iloc[0]
          App_old=(App_old.astype(int)).iloc[0]
          
          Gate_new=new.loc[new.DATEMAILED == i, 'GATE']
          Gate_old=old.loc[old.DATEMAILED == str(i), 'GATE']
          Gate_diff = (Gate_new.astype(int)-Gate_old.astype(int)).iloc[0]
          Gate_new=(Gate_new.astype(int)).iloc[0]
          Gate_old=(Gate_old.astype(int)).iloc[0]

          Fund_new=new.loc[new.DATEMAILED == i, 'FUND']
          Fund_old=old.loc[old.DATEMAILED == str(i), 'FUND']
          Fund_diff = (Fund_new.astype(int)-Fund_old.astype(int)).iloc[0]
          Fund_new=(Fund_new.astype(int)).iloc[0]
          Fund_old=(Fund_old.astype(int)).iloc[0]
          
          New_record = pd.DataFrame([[i,Mailed_new,Response_new,App_new,Gate_new,Fund_new,'',Report_Date_old,i,Mailed_old,Response_old,App_old,Gate_old,Fund_old,Mailed_diff,Response_diff,App_diff,Gate_diff,Fund_diff]],columns=['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff'])

          Old_record = pd.concat([Old_record,New_record])
     else:

          Mailed_new=new.loc[new.DATEMAILED == i, 'MAILED']
          Mailed_new=(Mailed_new.astype(int)).iloc[0]

          Response_new=new.loc[new.DATEMAILED == i, 'RESPONSE']
          Response_new=(Response_new.astype(int) - (Response_new.astype(int) - Response_new.astype(int))).iloc[0]

          App_new=new.loc[new.DATEMAILED == i, 'APP']
          App_new=(App_new.astype(int)).iloc[0]

          Gate_new=new.loc[new.DATEMAILED == i, 'GATE']
          Gate_new=(Gate_new.astype(int)).iloc[0]

          Fund_new=new.loc[new.DATEMAILED == i, 'FUND']
          Fund_new=(Fund_new.astype(int)).iloc[0]
          
          New_record = pd.DataFrame([[i,Mailed_new,Response_new,App_new,Gate_new,Fund_new,'','','','','','','','','','','','','']],columns=['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff'])
          Old_record = pd.concat([Old_record,New_record])
  

Old_record = pd.concat([Old_record,New_record])

############################################  Building Difference Report Ends  #################################################################################################################################

#################################################   Saving Sum Row to DataFrame   ############################################################################################################################
Qc_Old_record = Old_record.replace(r'\s*', np.nan, regex=True)
Qc_Old_record.fillna(0, inplace=True)

####   Adding sum row to the datfarme ######

sum_record = pd.DataFrame([["Totals",str(Qc_Old_record['Mailed_New'].sum().astype(int)),str(Qc_Old_record['Response_New'].sum().astype(int)),str(Qc_Old_record['App_New'].sum().astype(int)),str(Qc_Old_record['Gate_New'].sum().astype(int)),str(Qc_Old_record['Fund_New'].sum().astype(int)),'','','',str(Qc_Old_record['Mailed_Old'].sum().astype(int)),
str(Qc_Old_record['Response_Old'].sum().astype(int)),str(Qc_Old_record['App_Old'].sum().astype(int)),str(Qc_Old_record['Gate_Old'].sum().astype(int)),str(Qc_Old_record['Fund_Old'].sum().astype(int)),
str(Qc_Old_record['Mailed_Diff'].sum().astype(int)),str(Qc_Old_record['Response_Diff'].sum().astype(int)),str(Qc_Old_record['App_Diff'].sum().astype(int)),
str(Qc_Old_record['Gate_Diff'].sum().astype(int)),str(Qc_Old_record['Fund_Diff'].sum().astype(int))]],columns=['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff'])

Old_record_save = pd.concat([Old_record,sum_record])

#Old_record_save.to_csv('C:\\Deep\\Python\\Project_Som_Data\\QC_Result\\Som_Data_QC_Result_CO.csv',mode='w')

######################################################################## QC Checks starts Here ###############################################################################

QC_Check_Error_Message = "  \n Cash out  Errors starts here "

### Checking the Difference in Mailed Quanity
for i in range(0,Qc_Old_record['Mailed_Diff'].count()):
     if Qc_Old_record['Mailed_Diff'].iloc[i] > 500:
          QC_Check_Counter = 1
          QC_Check_Error_Message = QC_Check_Error_Message + "\n Mail difference for " + str(Qc_Old_record['DATEMAILED_New'].iloc[i])+ " is greater than expected." + " Mail difference is " + str(Qc_Old_record['Mailed_Diff'].iloc[i])

### Checking the Difference in Fund Quanity
for i in range(0,Qc_Old_record['Fund_Diff'].count()):
     if Qc_Old_record['Fund_Diff'].iloc[i] > 50:
          QC_Check_Counter = 1
          QC_Check_Error_Message = QC_Check_Error_Message + "\n Fund difference for " + str(Qc_Old_record['DATEMAILED_New'].iloc[i])+ " is greater than expected." + " Fund difference is " + str(Qc_Old_record['Fund_Diff'].iloc[i])
        
### CHECKING NEW MAILED, RESPONSE, APP, GATE, FUND COUNTS ARE HIGHER OR SAME AS COMPARED TO PREVIOUS  
if Qc_Old_record['Mailed_New'].sum() < Qc_Old_record['Mailed_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message = QC_Check_Error_Message + "\n New total Mailed Quantity " + str(Qc_Old_record['Mailed_New'].sum()) + " is less than previous reported count " + str(Qc_Old_record['Mailed_Old'].sum())

if Qc_Old_record['Response_New'].sum() < Qc_Old_record['Response_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message =  QC_Check_Error_Message + "\n New total Response Quantity " + str(Qc_Old_record['Response_New'].sum()) + " is less than previous reported count " + str(Qc_Old_record['Response_Old'].sum())
     
if Qc_Old_record['App_New'].sum() < Qc_Old_record['App_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message =  QC_Check_Error_Message + "\n New total App Quantity " + str(Qc_Old_record['App_New'].sum()) + " is less than previous reported count " + str(Qc_Old_record['App_Old'].sum())

if Qc_Old_record['Gate_New'].sum() < Qc_Old_record['Gate_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message =  QC_Check_Error_Message + "\n New total Gate Quantity " + str(Qc_Old_record['Gate_New'].sum()) + " is less than previous reported count " + str(Qc_Old_record['Gate_Old'].sum())

if Qc_Old_record['Fund_New'].sum() < Qc_Old_record['Fund_Old'].sum():
     QC_Check_Counter=1
     QC_Check_Error_Message =  QC_Check_Error_Message + "\n New total Fund Quantity " + str(Qc_Old_record['Fund_New'].sum()) + " is less than previous reported total mailed quanity " + str(Qc_Old_record['Fund_Old'].sum())

Qc_Old_record['Mailed_New'].sum()
Qc_Old_record['Response_New'].sum()
Qc_Old_record['App_New'].sum()
Qc_Old_record['Gate_New'].sum()
Qc_Old_record['Fund_New'].sum()
Qc_Old_record['Mailed_Old'].sum()
Qc_Old_record['Response_Old'].sum()
Qc_Old_record['App_Old'].sum()
Qc_Old_record['Gate_Old'].sum()
Qc_Old_record['Fund_Old'].sum()
Qc_Old_record['Mailed_Diff'].sum()
Qc_Old_record['App_Diff'].sum()
Qc_Old_record['Gate_Diff'].sum()
Qc_Old_record['Fund_Diff'].sum()
      
############################################################################################ Writing report to Excel ###########################################################################################

writer = pd.ExcelWriter("C:\\Users\\Deependra\\Som_Data_New\\Som_Data_QC_Result_CO.xlsx", engine='xlsxwriter')
# Convert the dataframe to an XlsxWriter Excel object.

#Old_record_print=Old_record_save[['DATEMAILED_New','Mailed_New','Response_New','App_New','Gate_New','Fund_New','Seperator Column','Report_Date_Old','DATEMAILED_Old','Mailed_Old','Response_Old','App_Old','Gate_Old','Fund_Old','Mailed_Diff','Response_Diff','App_Diff','Gate_Diff','Fund_Diff']]
Old_record_print=Old_record_save.iloc[1:,:]
Old_record_print.to_excel(writer, sheet_name='QC_Report')

# Get the xlsxwriter workbook and worksheet objects.
workbook  = writer.book
worksheet = writer.sheets['QC_Report']

# Add some cell formats.

format3 = workbook.add_format({'bold': True,'align':'center'})

format2 = workbook.add_format({'align':'center'})
#format3.set_border(4)

# Set the column width and format.
worksheet.set_column('K:T', 13, format2)
worksheet.set_column('C:G', 13, format2)
worksheet.set_column('I:J', 16, format2)
worksheet.set_column('B:B', 16, format2)
worksheet.set_row(0,20, format3)
worksheet.set_row(-1,20, format3)

# Close the Pandas Excel writer and output the Excel file.
writer.save()

########################################################################## Email Notification of QC ###################################################################################
if QC_Check_Counter == 1:
     print(QC_Check_Error_Message)
     text_file = open("C:\\Users\\Deependra\\Som_Data_New\\Error_Log.txt", "w")
     text_file.write(QC_Check_Error_Message)
     text_file.close()
     SendEmail(QC_Check_Error_Message) 
else:
     print("""  Starting the  Report generation process  """)
     text_file = open("C:\\Users\\Deependra\\Som_Data_New\\Error_Log.txt", "w")
     text_file.write(" No QC Errors found ")
     text_file.close()
     os.system('C:\\Deep\\Python\Project_Som_Data\\Python_Som_Data_Main.py') 

############################################################################################ End ###########################################################################################
