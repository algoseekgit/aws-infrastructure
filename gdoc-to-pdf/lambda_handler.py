import os
import json
import pathlib

import boto3
import requests

from botocore.exceptions import ClientError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# metadata API connection
url_prefix = 'https://metadata-services.algoseek.com/api/v1/'
metadata_api_name = os.getenv('metadata_api_name')
metadata_api_secret = os.getenv('metadata_api_secret')


session = boto3.Session()
s3 = session.resource('s3')


download_location = pathlib.Path('/tmp/')


def get_secret(secret_name: str, region_name: str = 'us-east-1') -> dict:

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)


def get_metadata_api_headers() -> dict:
    
    auth_token_url = url_prefix + 'login/access_token/'
    payload = {
        'name': metadata_api_name,
        'secret': metadata_api_secret
    }

    r = requests.post(auth_token_url, data=json.dumps(payload))
    assert r.status_code == 200, r.text
    token = r.json()['token']

    headers = {"Authorization": f"Bearer {token}"}
    return headers


def get_dataset_documentation_info(dataset_text_id: str, auth_headers: dict) -> dict:
    
    url = url_prefix + f'internal/dataset/text_id/{dataset_text_id}/'
    r = requests.get(url, headers=auth_headers)
    data = r.json()
    if r.status_code != 200:
        return {
            'status_code': r.status_code, 'message': data['detail'], 'data': {}
        }
    
    documentation_id = data['documentation_id']
    if documentation_id is None:
        return {
            'status_code': 404, 'message': 'Dataset documentation does not exist', 'data': {}
        }
    
    url = url_prefix + f'internal/documentation/ext/{documentation_id}/'
    r = requests.get(url, headers=auth_headers)
    data = r.json()
    if r.status_code != 200:
        return {
            'status_code': r.status_code, 'message': data['detail'], 'data': {}
        }
    
    error_messages = []
    if not data['source_path']:
        error_messages.append("Documentation source path is not set")
    if not data['s3_location']:
        error_messages.append("Documentation S3 destination is not set")
    
    if error_messages:
        return {
            'status_code': 422, 'message': '. '.join(error_messages), 'data': {}
        }

    doc_data = {
        'source_path': data['source_path'], 
        'bucket_name': data['s3_location']['bucket_name'], 
        'object_name': data['s3_location']['object_name']
    }
    return {'status_code': 200, 'message': 'OK', 'data': doc_data}
    
    
def retry(times, exceptions):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    :param Exceptions: Lists of exceptions that trigger a retry attempt
    :type Exceptions: Tuple of Exceptions
    """
    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    attempt += 1
            return func(*args, **kwargs)
        return newfn
    return decorator


@retry(times=2, exceptions=(Exception, ))
def download_as_pdf(service, file_id: str, output_filename: str) -> dict:
    request = service.files().export_media(fileId=file_id, mimeType='application/pdf')
    with open(output_filename, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.")



def generate_documentation_pdf(service, source_path: str, bucket_name: str, object_name: str) -> dict:
    
    if not source_path.startswith('https://docs.google.com/document/d/'):
        return {'status_code': 422, 'message': 'The source gdoc path is not properly formatted'}
        
    file_id = source_path.rsplit("/", 1)[1]
    local_filename = download_location / object_name
    
    try:
        download_as_pdf(service, file_id, local_filename)

        s3.meta.client.upload_file(
            str(local_filename), bucket_name, object_name,
            ExtraArgs={'ContentType': 'application/pdf', 'ContentDisposition': 'inline'}
        )
        return {'status_code': 200, 'message': 'OK'}
        
    except Exception as e:
        return {'status_code': 500, 'message': str(e)}


def lambda_handler(event, context):

    secret_name = "aws-lambda-gdoc-credentials"
    credentials = service_account.Credentials.from_service_account_info(
        get_secret(secret_name), scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    service = build('drive', 'v3', credentials=credentials)
    
    assert 'dataset_text_ids' in event

    response_list = []  # a list of statuses for each text id provided
    for text_id in event['dataset_text_ids']:
        response = get_dataset_documentation_info(text_id, get_metadata_api_headers())
        data_doc = response.pop('data')

        if response['status_code'] == 200:
            r = generate_documentation_pdf(
                service, data_doc['source_path'], 
                data_doc['bucket_name'], data_doc['object_name']
            )
            response_list.append(r)
        else:
            response_list.append(response)
    
    return response_list

