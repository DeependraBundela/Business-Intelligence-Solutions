import paramiko
import datetime

host = "sftp.freedommortgage.com"                    #hard-coded
port = 22



  
transport = paramiko.Transport((host, port))

password = "9xCLcvlR"                #hard-coded
username = "dmsftp"                #hard-coded
transport.connect(username = username, password = password)

sftp = paramiko.SFTPClient.from_transport(transport)

import sys
now = datetime.datetime.now()
Year = now.year
Month = now.month
Day = now.day
Current_Date= str(Year)+'_' + str(Month) + '_'+ str(Day)
Folder_Name= 'Som_Data_' + Current_Date
path = '/Data for Som/'+str(Folder_Name)+'.zip'
localpath = 'C:/Users/Deependra/Som_Data_Zip/'+str(Folder_Name)+'.zip'
sftp.put(localpath, path)

sftp.close()
transport.close()
print ('Upload done.')
