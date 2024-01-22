import configparser
import requests
import json
import pandas as pd
import boto3
import redshift_connector
import psycopg2
import logging
import csv
from sqlalchemy import create_engine
from botocore.exceptions import NoCredentialsError
from create import dev_table

config=configparser.ConfigParser()
config.read('.env')

access_key= config['AWS']['access_key']
secret_key= config['AWS']['secret_key']
bucket_name= config['AWS']['bucket_name']
region= config['AWS']['region']
local_file_path= config['AWS']['local_file_path']
s3_key= config['AWS']['s3_key']
role= config['AWS']['arn']

db_host=config['DB_CONN']['db_host']
db_user=config['DB_CONN']['db_user']
db_password=config['DB_CONN']['db_password']
db_database=config['DB_CONN']['db_database']

dwh_host=config['DWH_CONN']['host']
dwh_user=config['DWH_CONN']['user']
dwh_password=config['DWH_CONN']['password']
dwh_database=config['DWH_CONN']['database']
# #creating the raw data s3 bucket--step 1

# print(region, bucket_name)
# client = boto3.client(
#     's3',
#     aws_access_key_id=access_key,
#     aws_secret_access_key=secret_key
# )

# client.create_bucket(
#     Bucket=bucket_name,
#     CreateBucketConfiguration={
#         'LocationConstraint': region,
#     },
# )

# # raw data S3 bucket information

# def upload_to_s3(local_file_path, s3_key):
#     s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

#     try:
#         s3.upload_file(local_file_path, bucket_name, s3_key)
#         print(f"Upload successful: {local_file_path} to {bucket_name}/{s3_key}")
#     except FileNotFoundError:
#         print(f"The file {local_file_path} was not found.")
#     except NoCredentialsError:
#         print("Credentials not available or incorrect.")

# def download_from_s3(s3_key, local_file_path):
#     s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

#     try:
#         s3.download_file(bucket_name, s3_key, local_file_path)
#         print(f"Download successful: {bucket_name}/{s3_key} to {local_file_path}")
#     except NoCredentialsError:
#         print("Credentials not available or incorrect.")
#     except FileNotFoundError:
#         print(f"The file {bucket_name}/{s3_key} was not found.")

# # Upload to S3
# upload_to_s3(local_file_path, s3_key)


# #  Download from S3
# downloaded_file_path = r'C:\Users\DELL 5480\Desktop\INTERSHIP_10ALYTICS\downloaded_dataengineering_jobs.json'
# download_from_s3(s3_key, downloaded_file_path)


# def json_to_csv(json_file, csv_file):
#     with open(json_file, 'r') as file:
#         data = json.load(file)

#     df = pd.json_normalize(data)

#     # Save the DataFrame to a CSV file
#     df.to_csv(csv_file, index=False)

# json_to_csv('C:/Users/DELL 5480/Desktop/INTERSHIP_10ALYTICS/downloaded_dataengineering_jobs.json', 'C:/Users/DELL 5480/Desktop/INTERSHIP_10ALYTICS/output.csv')

# #DATA TRANSFORMATION STEPS

# #creating the transformed data s3 bucket--step 1

access_key2 = config['AWS']['access_key2']
secret_key2 = config['AWS']['secret_key2']
bucket_name2 = config['AWS']['bucket_name2']
region2 = config['AWS']['region2']
local_file_path2 = config['AWS']['local_file_path2']
s3_key2 = config['AWS']['s3_key2']

# print(region2, bucket_name2)
# client = boto3.client(
#     's3',
#     aws_access_key_id=access_key,
#     aws_secret_access_key=secret_key
# )

# client.create_bucket(
#     Bucket=bucket_name2,
#     CreateBucketConfiguration={
#         'LocationConstraint': region2,
#     },
# )

# #S3 bucket information for transformed data

# def select_and_save(dataengineering_jobs, transformed_job_data):
#     with open(dataengineering_jobs, 'r') as file:
#         data = json.load(file).get('data', [])  #this extracts the 'data' key from the json file and handle missing key
    
# # To create a DataFrame from the data
#     df = pd.DataFrame(data)
    
# #only the columns to keep
#     selected_columns = ['employer_website', 'job_id', 'job_employment_type', 'job_title', 'job_apply_link', 'job_description', 'job_city', 'job_country', 'job_posted_at_timestamp', 'employer_company_type']
#     df_selected = df[selected_columns]
#     print(df_selected)

# #to save the transformed DataFrame to a CSV file
#     df_selected.to_csv(transformed_job_data, index=False)
#     print(f"Transformed data saved to {transformed_job_data}")

# # to upload the transformed data to S3
#     upload_to_s3(transformed_job_data, s3_key2)
# # the function to upload to S3
# def upload_to_s3(local_file_path, s3_key):
#     s3 = boto3.client('s3', aws_access_key_id=access_key2, aws_secret_access_key=secret_key2)
    
#     try:
#         s3.upload_file(local_file_path, bucket_name2, s3_key)
#         print(f"Upload successful: {local_file_path} to {bucket_name2}/{s3_key}")
#     except FileNotFoundError:
#         print(f"The file {local_file_path} was not found.")
#     except NoCredentialsError:
#         print("Credentials not available or incorrect.")

# select_and_save('dataengineering_jobs.json', 'transformed_job_data.csv')
# conn = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_database}")


db_table = ['dataengineering_jobs']

# for table in db_table:
#     query =f'SELECT * FROM {table}'
#     logging.info(f'======== Executing {query}')
#     df= pd.read_sql_query(query, conn)

#     df.to_csv(
#         f's3://{bucket_name2}/{table}.csv',
#         index= False,
#         storage_options= {
#             'key': access_key,
#             'secret': secret_key}
#     )


# # To pull data from the datalake to the data warehous
dwh_conn = redshift_connector.connect(
    host=dwh_host,
    database=dwh_database,
    user=dwh_user,
    password=dwh_password,
    timeout= 40
 )
print ('DWH connection is established')
cursor=dwh_conn.cursor()

dev_schema = 'dev'
staging_schema= 'redshift'
cursor.execute(f'''CREATE SCHEMA {dev_schema};
CREATE TABLE IF NOT EXISTS dataengineering_jobs 
(job_id character varying PRIMARY KEY NOT NULL,
employer_website character varying,
job_employment_type character varying
job_title character varying,
job_apply_link character varying,
job_description character varying,
job_city text,
job_country text,
job_posted_at_timestamp bigint,
employer_company_type character varying
);''')
print ('DWH connection established')


#-- Create a dev schema
cursor.execute(f'''CREATE SCHEMA {dev_schema}:''')
dwh_conn.commit()

#--Create the tables
for query in dev_table:
    print(f'......... {query[:30]}')
    cursor.execute(query)
    dwh_conn.commit()

#-- copy tables from s3
for table in db_table:
    cursor.execute(f'''
        COPY {dev_schema}.{table}
        FROM 's3://{bucket_name2}/{table}.csv'
        IAM_role '{role}'
        DELIMITER ','
        IGNOREHEADER 1;
''')  
dwh_conn.commit()

cursor.close()
dwh_conn.close()


