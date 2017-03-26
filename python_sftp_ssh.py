import paramiko
import datetime
import os
import sys

host = "sftp.abc.com"                    #hard-coded
port = 22
password = "password"                #hard-coded
username = "username"                #hard-coded

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#In case the server's key is unknown,
#we will be adding it automatically to the list of known hosts
#ssh.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
#Loads the user's local known host file.
ssh.connect(host, username=username, password=password)
ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('ls /tmp')
print ("output"), ssh_stdout.read() #Reading output of the executed command
error = ssh_stderr.read()
#Reading the error stream of the executed command
print ("err"), error, len(error) 

now = datetime.datetime.now()
Year = now.year
Month = now.month
Day = now.day
Current_Date= str(Year)+'_' + str(Month) + '_'+ str(Day)
Folder_Name= 'Som_Data_' + Current_Date
path = '/Data for Som/'+str(Folder_Name)+'.zip'
localpath = 'C:/Users/Deependra/Som_Data_Zip/Som_Data_2017_1_31.zip'
sftp.put(localpath, path)

sftp.close()
print ('Upload done.')
