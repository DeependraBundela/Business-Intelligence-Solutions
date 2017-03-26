#!/usr/bin/env python
import os
import zipfile
import datetime

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

if __name__ == '__main__':
    now = datetime.datetime.now()
    Year = now.year
    Month = now.month
    Day = now.day
    Current_Date= str(Year)+'_' + str(Month) + '_'+ str(Day)
    Folder_Name= 'Som_Data_' + Current_Date
    Folder_Path= 'C:/Users/Deependra/Som_Data_Zip/'+str(Folder_Name)+'.zip'
    zipf = zipfile.ZipFile(Folder_Path, 'w', zipfile.ZIP_DEFLATED)
    zipdir('C:/Users/Deependra/Som_Data_New', zipf)
    zipf.close()
