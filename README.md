# Business-Intelligence-Solutions

Descrption:
This project is a fully automated BI solution for Pulling huge data from snowflake and sftp it to some other site. This Process involves Qc automation process, Data extraction process, Zipping process, Sftp of data and Email alerts system. 

QC Involves:
1. In this process, before extracting data, first current report is pulled and compared with previously sent report and a comparion report is prepared using using Python Pandas. On this comparision report lot of business rules are applied to check if  there are any anamolies in the result. If there are anmolies detiled anamolies email is sent to stakeholders. 
If there are no anamolies data is extracted from snowflake, zipped and sent to sftp location and email is sent to respective respective stakeholders.

Steps to use:
1. Run 


