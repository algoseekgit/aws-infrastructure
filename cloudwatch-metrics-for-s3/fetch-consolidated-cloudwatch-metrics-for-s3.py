#
# Gets total bucket size and the number of ojbects
#     in all account's buckets
#
# usage: python3 list_buckets.py
#        python3 list_buckets.py Jan 2019
#        python3 list_buckets.py May 2018
# 
#  

import sys
import csv
import datetime
import os

import boto3


def get_metric_from_response(response):
    datapoints = response['Datapoints']
    if datapoints:
        return int(datapoints[0]['Maximum'])
    else:
        return 0

def lambda_handler(event, context):
    account_name = event['account_name']
    storage_types = {
        'size_standard': 'StandardStorage',
        'size_standard_ia': 'StandardIAStorage',
        'size_one_zone_ia': 'OneZoneIAStorage',
        'size_glacier_instant': 'GlacierInstantRetrievalStorage',
        'size_glacier': 'GlacierStorage', 
        'size_deep_archive': 'DeepArchiveStorage'
    }
    
    period = 3600   # 1 hour


    if not 'date' in event:
        today = datetime.datetime.today()
    else:
        today = datetime.datetime.strptime(event['date'], '%Y-%m-%d')
    
    # get metric as for the start of the month  
    metric_start_date = datetime.datetime(today.year, today.month, 1)
    metric_end_date = metric_start_date + 5*datetime.timedelta(seconds=period)
    output_filename = metric_start_date.strftime(f'/tmp/%Y-%m-{account_name}.csv')

    fieldnames = ["bucket_name", "account_name", "creation_date", "total_objects"] + list(storage_types)
    with open(output_filename, 'w') as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        
        # 
        if '-' in account_name:
            underscored_account_name = account_name.replace('-', '_')
            account_access_key = os.getenv(underscored_account_name + '_account_access_key')
            secret_access_key = os.getenv(underscored_account_name + '_secret_access_key')
        else:
            account_access_key = os.getenv(account_name + '_account_access_key')
            secret_access_key = os.getenv(account_name + '_secret_access_key')
        session = boto3.Session(aws_access_key_id=account_access_key, 
                                aws_secret_access_key=secret_access_key, )
        
        s3 = session.resource('s3')
        
        cloudwatch = session.resource('cloudwatch')
        size_metric = cloudwatch.Metric('AWS/S3', 'BucketSizeBytes')
        objects_metric = cloudwatch.Metric('AWS/S3', 'NumberOfObjects')
        
        for bucket in s3.buckets.all():
            print(bucket.name)
            response = objects_metric.get_statistics(
                Dimensions=[
                    {'Name': 'BucketName', 'Value': bucket.name},
                    {'Name': 'StorageType', 'Value': 'AllStorageTypes'},
                ],
                StartTime=metric_start_date, EndTime=metric_end_date, 
                Period=period, Statistics=['Maximum']
            )
            bucket_num_objects = get_metric_from_response(response)
            
            payload = {
                "bucket_name": bucket.name, 
                "account_name": account_name,
                "creation_date": bucket.creation_date.strftime('%Y-%m-%d'), 
                "total_objects": bucket_num_objects,
            }
            
            for key, storage_type in storage_types.items():
                
                response = size_metric.get_statistics(
                    Dimensions=[
                        {'Name': 'BucketName', 'Value': bucket.name},
                        {'Name': 'StorageType', 'Value': storage_type},
                    ],
                    StartTime=metric_start_date, EndTime=metric_end_date, 
                    Period=period, Statistics=['Maximum']
                )
                bucket_size = get_metric_from_response(response)
                payload[key] = bucket_size
            
            writer.writerow(payload)
    
    s3_current = boto3.resource('s3')
    s3_current.Bucket('as-cloudwatch-metrics-for-s3').upload_file(output_filename, metric_start_date.strftime(f'%Y/%Y-%m/%Y-%m-{account_name}.csv'))
    return 0    
        
