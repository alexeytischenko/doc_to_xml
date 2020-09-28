# -*- coding: utf-8 -*-

"""
AWS lambda converts office documents like doc, docx, odt to xml (html or txt)
"""

import urllib
import boto3
import uuid
import os

s3 = boto3.resource('s3')

# copy LO from s3 to lambda environment tmp and untar it
os.system(
    "curl https://df-archive-dev2.s3.eu-central-1.amazonaws.com/lo.tar.gz -o /tmp/lo.tar.gz && "
    "cd /tmp && file lo.tar.gz && tar xzf lo.tar.gz")


# create object from string without triggering s3 object created event
def create_object_without_event_str(file_str, key, bucket):
    temp_key = "tmp/" + str(uuid.uuid4())
    s3.Bucket(bucket).put_object(Key=temp_key, Body=file_str)
    s3.Object(bucket, key).copy_from(CopySource=bucket + '/' + temp_key)
    s3.Object(bucket, temp_key).delete()


def lambda_handler(event, context):
    # get info from event
    # s3 bucket
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_bucket = boto3.resource("s3").Bucket(bucket_name)

    # source file
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    _, input_file_name = os.path.split(key)

    # converted file
    ext = event['Records'][0]['s3']['object']['to']
    new_key = key.rsplit('.')[0] + '.' + ext

    # get file to be converted from s3
    with open("/tmp/{}".format(input_file_name), 'wb') as data:
        s3_bucket.download_fileobj(key, data)

    # Execute libreoffice to convert input file
    convert_command = "\"instdir/program/soffice.bin\"" \
                      " --headless --invisible --nodefault --nofirststartwizard --nolockcheck --nologo --norestore " \
                      "--convert-to xml --outdir /tmp"
    os.system("cd /tmp && {} {}".format(convert_command, input_file_name))

    # Save converted object back to S3
    output_file_name, _ = os.path.splitext(input_file_name)
    output_file_name = output_file_name + "." + ext
    f = open("/tmp/{}".format(output_file_name), "rb")
    create_object_without_event_str(f.read(), new_key, bucket_name)
    f.close()
