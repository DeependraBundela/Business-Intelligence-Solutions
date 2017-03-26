#### Main Process File ########

import os
from subprocess import *
from Python_Office365_Mail import SendEmail

print(" ########################################## ")
print(" Data Extraction - STARTED  ")
Log_status_Message= " Data Extraction - STARTED "
#run child script 1 Extract all the data
p = Popen([r'C:\Deep\Python\Project_Som_Data\Python_export_txt_STML_CO_Data_Pull.py'], shell=True, stdin=PIPE, stdout=PIPE)
output = p.communicate()
print (output[0])
print(" Data Extraction - DONE ")
Log_status_Message=Log_status_Message + "\n Data Extraction - DONE "

print(" ########################################## ")
print(" Data Zip - STARTED  ")
Log_status_Message=Log_status_Message + "\n Data Zip - STARTED "
#run child script 3 ALL file Zipped
p = Popen([r'C:\Deep\Python\Project_Som_Data\Python_Zip.py'], shell=True, stdin=PIPE, stdout=PIPE)
output = p.communicate()
print (output[0])
print(" Data Zip - DONE  ")
Log_status_Message=Log_status_Message + "\n Data Zip - DONE "

print(" ########################################## ")
print(" Data SFTP - STARTED  ")
Log_status_Message=Log_status_Message + "\n Data SFTP - STARTED "
#run child script 4 Files uploaded on sftp Location
p = Popen([r'C:\Deep\Python\Project_Som_Data\python_paramiko_sftp.py'], shell=True, stdin=PIPE, stdout=PIPE)
output = p.communicate()
print (output[0])
print(" Data SFTP - DONE  ")
Log_status_Message=Log_status_Message + "\n Data SFTP - DONE "

print(" ########################################## ")
#run child script 5 Emailed Status
print(" EMAIL - SENT STARTED ")
Log_status_Message=Log_status_Message + "\n EMAIL - SENT STARTED "
p = SendEmail(Log_status_Message)
print(" EMAIL - SENT  DONE ")

