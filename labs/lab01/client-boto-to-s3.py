#
# Downloads image from S3 using AWS's boto3 library
#
import logging
import sys

import boto3  # access to Amazon Web Services (AWS)
from botocore import UNSIGNED
from botocore.client import Config

from configparser import ConfigParser

#
# eliminate traceback so we just get error message:
#
sys.tracebacklimit = 0

try:
    print("**Starting**")
    print()
     
    #
    # setup AWS based on config file:
    #
    config_file = 's3-config.ini'    
    configur = ConfigParser()
    configur.read(config_file)

    #
    # get web server URL from config file:
    #
    bucket_name = configur.get('bucket', 'bucket_name')
    region_name = configur.get('bucket', 'region_name')

    #
    # gain access to CS 310's public photoapp bucket:
    #
    s3 = boto3.resource(
      's3',
      region_name=region_name,
      # enables access to public objects:
      config=Config(retries = {'max_attempts': 3, 'mode': 'standard'}, signature_version=UNSIGNED))

    bucket = s3.Bucket(bucket_name)

    #
    # Download image requested by user:
    #
    imagename = input("Enter image to download without extension> ")
    print()
    object_metadata = s3.Object(bucket_name, imagename).content_type
    parsed_content_type = "."
    if 'jpeg' in object_metadata:
      parsed_content_type += 'jpg'
    elif 'text/plain' in object_metadata:
      parsed_content_type += 'txt'
    elif 'python' in object_metadata:
      parsed_content_type += 'py'
    elif 'application' in object_metadata:
      parsed_content_type += object_metadata.split('/')[1]
    else:
      parsed_content_type += 'unknown'
  
    local_filename = imagename + parsed_content_type

    bucket.download_file(imagename, local_filename)
    
    print(f"Success, image downloaded to '{local_filename}'")

    print()
    print("**Done**")

except Exception as err:
  print()
  print(f"ERROR:\n Bucket: {bucket_name} \n Region: {region_name} \n Msg: {err}")

