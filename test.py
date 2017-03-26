import os
from subprocess import *
from Python_Office365_Mail import SendEmail

print(" ########################################## ")
print(" Data Extraction - STARTED  ")
Log_status_Message= " Data Extraction - STARTED "
#run child script 1 Extract all the data
p = Popen([r'C:\Deep\Python\help.py'], shell=True, stdin=PIPE, stdout=PIPE)
output = p.communicate()
print (output[0])
print(" Data Extraction - DONE ")
Log_status_Message=Log_status_Message + "\n Data Extraction - DONE "

print(Log_status_Message)

print(" ########################################## ")
#run child script 5 Emailed Status
print(" EMAIL - SENT STARTED ")
Log_status_Message=Log_status_Message + "\n EMAIL - SENT STARTED "
p = SendEmail(Log_status_Message)
print (output[0])
print(" EMAIL - SENT  DONE ")

