import boto3
import logging
import os
import time

# Read list of existing bukets in S3
def list_bucket():
    #Create bucket
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        if response:
            print('Existing buckets in s3: ')
            for bucket in response['Buckets']:
                print(f'  {bucket["Name"]}')
    except Exception as e:
        logging.error(e)
        return False
    return True

#list_bucket() #Calling the function for output

#Create AWS S3 bucket python boto3
def create_bucket(bucket_name, region = None):
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket = bucket_name)
        else:
            s3_client = boto3.client('s3', region_name = region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket = bucket_name, CreateBucketConfiguration = location)
    except Exception as e:
        logging.error(e)
        return False
    return True
'''
#For unique bucket name
unique_bucket_name = f"s3-sahana-test-{int(time.time())}"
#Calling Create Bucket function
result = create_bucket(unique_bucket_name)
if result:
    print("Bucket successfully created!")
else:
    print("Bucket is not created")
'''

#Uploading a file into created bucket
def upload_file(file_name, bucket, object_name=None):
    #If S3 object name was not specified we will use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)
    
    #Uploading the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name,bucket, object_name)
    except Exception as e:
        logging.error(e)
        return False
    return True
#Calling Upload file function
'''
upload_result = upload_file("C:\\Users\\Sahana\\OneDrive\\Pictures\\Camera Roll\\WIN_20230911_13_48_26_Pro.JPG", 's3-sahana-test-1728761004', 'WIN_20230911_13_48_26_Pro.JPG')
if upload_result:
    print("File is uploaded successfully..!")
else:
    print("File upload is failed..!")
'''
#Downloading a file from bucket
def download_file(file_name, bucket, object_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(bucket,object_name,file_name)
    except Exception as e:
        logging.error(e)
        return False
    return True
#Calling download file function
'''
download_result = download_file("C:\\Users\\Sahana\\OneDrive\\Pictures\\Camera Roll\\WIN_20230911_13_48_26_Pro.JPG", 's3-sahana-test-1728761004', 'WIN_20230911_13_48_26_Pro.JPG')
if download_result:
    print("File is downloaded successfully..!")
else:
    print("File download is failed..!")
'''

#Deleting file from a bucket
def delete_file(bucket, key_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.delete_object(Bucket=bucket,Key = key_name)
    except Exception as e:
        logging.error(e)
        return False
    return True

#Calling delete file funtion
'''
delete_result = delete_file('s3-sahana-test-1728761004', 'WIN_20230911_13_48_26_Pro.JPG')
if delete_result:
    print("File successfully deleted..!")
else:
    print("File not deleted..!")
'''

#Delete a bucket from s3
def delete_bucket(bucket):
    s3_client = boto3.client('s3')
    try:
        s3_client.delete_bucket(Bucket = bucket)
    except Exception as e:
        logging.error(e)
        return False
    return True

#Calling Delete bucket function
'''
delete_result = delete_bucket('dataeng-s3')
if delete_result:
    print("Bucket successfully deleted..!")
else:
    print("Bucket not deleted..!")
'''
list_bucket()