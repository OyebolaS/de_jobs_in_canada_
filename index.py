import configparser
import requests
import json
import pandas as pd
import boto3
import configparser
import psycopg2
import csv
from sqlalchemy import create_engine
from botocore.exceptions import NoCredentialsError


config=configparser.ConfigParser()
config.read('.env')

access_key= config['AWS']['access_key']
secret_key= config['AWS']['secret_key']
bucket_name= config['AWS']['bucket_name']
region= config['AWS']['region']
local_file_path= config['AWS']['local_file_path']
s3_key= config['AWS']['s3_key']

db_host=config['DB_CONN']['host']
db_user=config['DB_CONN']['user']
db_password=config['DB_CONN']['password']
db_database=config['DB_CONN']['database']


#creating the raw data s3 bucket--step 1

print(region, bucket_name)
client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

client.create_bucket(
    Bucket=bucket_name,
    CreateBucketConfiguration={
        'LocationConstraint': region,
    },
)

# raw data S3 bucket information

def upload_to_s3(local_file_path, s3_key):
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Upload successful: {local_file_path} to {bucket_name}/{s3_key}")
    except FileNotFoundError:
        print(f"The file {local_file_path} was not found.")
    except NoCredentialsError:
        print("Credentials not available or incorrect.")

def download_from_s3(s3_key, local_file_path):
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    try:
        s3.download_file(bucket_name, s3_key, local_file_path)
        print(f"Download successful: {bucket_name}/{s3_key} to {local_file_path}")
    except NoCredentialsError:
        print("Credentials not available or incorrect.")
    except FileNotFoundError:
        print(f"The file {bucket_name}/{s3_key} was not found.")

# Upload to S3
upload_to_s3(local_file_path, s3_key)


#  Download from S3
downloaded_file_path = r'C:\Users\DELL 5480\Desktop\INTERSHIP_10ALYTICS\downloaded_dataengineering_jobs.json'
download_from_s3(s3_key, downloaded_file_path)


def json_to_csv(json_file, csv_file):
    with open(json_file, 'r') as file:
        data = json.load(file)

    df = pd.json_normalize(data)

    # Save the DataFrame to a CSV file
    df.to_csv(csv_file, index=False)

json_to_csv('C:/Users/DELL 5480/Desktop/INTERSHIP_10ALYTICS/downloaded_dataengineering_jobs.json', 'C:/Users/DELL 5480/Desktop/INTERSHIP_10ALYTICS/output.csv')

DATA TRANSFORMATION STEPS

#creating the transformed data s3 bucket--step 1

print(region2, bucket_name2)
client = boto3.client(
    's3',
    aws_access_key_id=access_key2,
    aws_secret_access_key=secret_key2
)

client.create_bucket(
    Bucket=bucket_name2,
    CreateBucketConfiguration={
        'LocationConstraint': region2,
    },
)

#S3 bucket information for transformed data
access_key2 = config['AWS']['access_key2']
secret_key2 = config['AWS']['secret_key2']
bucket_name2 = config['AWS']['bucket_name2']
region2 = config['AWS']['region2']
local_file_path2 = config['AWS']['local_file_path2']
s3_key2 = config['AWS']['s3_key2']

def select_and_save(dataengineering_jobs, transformed_job_data):
    with open(dataengineering_jobs, 'r') as file:
        data = json.load(file).get('data', [])  #this extracts the 'data' key from the json file and handle missing key
    
# To create a DataFrame from the data
    df = pd.DataFrame(data)
    
#only the columns to keep
    selected_columns = ['employer_website', 'job_id', 'job_employment_type', 'job_title', 'job_apply_link', 'job_description', 'job_city', 'job_country', 'job_posted_at_timestamp', 'employer_company_type']
    df_selected = df[selected_columns]
    print(df_selected)

#to save the transformed DataFrame to a CSV file
    df_selected.to_csv(transformed_job_data, index=False)
    print(f"Transformed data saved to {transformed_job_data}")

# to upload the transformed data to S3
    upload_to_s3(transformed_job_data, s3_key2)
# the function to upload to S3
def upload_to_s3(local_file_path, s3_key):
    s3 = boto3.client('s3', aws_access_key_id=access_key2, aws_secret_access_key=secret_key2)
    
    try:
        s3.upload_file(local_file_path, bucket_name2, s3_key)
        print(f"Upload successful: {local_file_path} to {bucket_name2}/{s3_key}")
    except FileNotFoundError:
        print(f"The file {local_file_path} was not found.")
    except NoCredentialsError:
        print("Credentials not available or incorrect.")

select_and_save('dataengineering_jobs.json', 'transformed_job_data.csv')






