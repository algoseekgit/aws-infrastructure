"""
Author: Andrii <andrii.baldyniuk@algoseek.com>
Create Date: 10/10/2023
Title: invoke-consolidated-cloudwatch-metrics-for-s3
Description: this script provides iteration through the list of AWS accounts and
            invocation of another lambda function with event containing the name
            of dedicated account
"""

import json
import boto3

client = boto3.client('lambda')

def lambda_handler(event, context):
    default_accounts = ['data', 'qgdata', 'datalake', 'opra_dev', 'opra_analytics', 
                'index', 'equityrefdata', 'futuresrefdata', 'china-data', 
                'data_exchange', 'algoseek-docs', 'algoseek-sample', 
                'equity-market-data-02', 'equityrefdata-02']
    accounts = event.get('account_names', default_accounts)
    
    for account in accounts:
        inputJson = {'account_name': account}
        if 'date' in event:
            inputJson['date'] = event['date']
        response = client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:081913606759:function:fetch-consolidated-cloudwatch-metrics-for-s3',
            InvocationType='Event',
            Payload=json.dumps(inputJson)
            )
    
    return 0
