# Final Assignment - Group Project

## Python ETL:
Automatically pull data from .csv files, perform data cleaning operations, apply standardized transformations, and upload the processed data to a MySQL Workbench database.

## Group members:
[Juan Jose Montesinos](https://www.linkedin.com/in/jmont90/)\
[Maximilian von Braun](https://www.linkedin.com/in/maximilian-von-braun-b714b624b/)\
[Jakob Spranger](https://www.linkedin.com/in/jakob-spranger-3396b2170/)\
[Massimiliano Napolitano](https://www.linkedin.com/in/massimiliano-nap/)

## How to run the ETL process:
1. Go into the *creds.yaml* file and configure the database address, host, username, and password to your target database in MySQL Workbench

2. Make sure the following .csv files are present in this folder. The filenames are as follows:\
a. sales_phases_funnel.csv\
b. meteo_eae.csv\
c. zipcode_eae.csv

3. Run the *lead_to_sale_ETL.py* file to perform the ETL process

4. After completion, you should see the new tables in your MySQL Workbench database

5. You can find the SQL queries to answer the business problems in the PDF file containing the data model
