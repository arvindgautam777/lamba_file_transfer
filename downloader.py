# pylint: disable=missing-module-docstring
import requests
import os
from dotenv import load_dotenv
import boto3
import xmltodict
import pandas as pd

load_dotenv()

env = os.environ.get('ENVIRONMENT')
access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket = os.getenv('BUCKET_NAME')
file_prefix = os.environ.get('FILE_PREFIX')

#file = '2021-01-29-0.json.gz'
#key_get = '2021-01-29-0-name.json.gz'


def download_file(file):
    print(f"Download started for file {file} ...................")
    res = requests.get(f'https://data.gharchive.org/{file}')
    print("Download completed for ", file)
    return res


def read_file(access_key, secret_access_key, bucket, key_get):
    s3_client = boto3.client('s3')
    read_res = s3_client.get_object(
                Bucket = bucket,
                Key = key_get
    )
    return read_res

def upload_file(access_key, secret_access_key, bucket, key, body):
    s3_client = boto3.client('s3')
    read_res = s3_client.put_object(
                Bucket = bucket,
                Key = key,
                Body = body
    )
    return read_res

def get_list_of_files():
    website_response = requests.get("https://data.gharchive.org")
    dict_response = xmltodict.parse(website_response.content)
    files_list_df = pd.DataFrame(dict_response['ListBucketResult']['Contents'])
    files_list_df = files_list_df.sort_values('Size')
    file_names = files_list_df['Key'].tolist()
    return file_names

def list_of_files_in_s3_bucket(bucket):
    #get the list of content inside the bucket
    s3_client = boto3.client('s3')
    bucket_content = s3_client.list_objects(Bucket=bucket)['Contents']

    #convert the content into the dataframe
    bucket_content_df = pd.DataFrame(bucket_content)
    bucket_file_list = bucket_content_df['Key'].tolist()
    return bucket_file_list


def transfer_file_from_website_to_s3(file):
    downloaded_file = download_file(file)
    body = downloaded_file.content
    key = file
    upload_status = upload_file(access_key, secret_access_key, bucket, key, body)
    return upload_status