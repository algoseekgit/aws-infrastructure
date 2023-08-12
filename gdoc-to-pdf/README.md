# Convertion of gdoc files into PDF files and uploading to the dedicated s3 bucket
​
​
**Description:** This code is created in order to access gdoc files located on Google Drive and generate PDF files based on the received information about bucket_name, object_name and source_path from algoseek Metadata API. The generated file then is loaded into the corresponding bucket.
First of all, a list of dataset text IDs that need update can be set through the lambda_handler event. After that, the code accesses Metadata API (credentials from environment) and Google API (credentials through AWS Secret Manager) and records the ID of the documentation that corresponds to the specified text_id. Then for documentation_id receives information about bucket_name, object_name and source_path of that documentation from Metadata_API.
The second step is to access Google Drive and download files in PDF format with the appropriate sourcs_path. Then they are uploaded to dedicated s3 bucket through their bucket_name and object_name.
After all the function returns a list of response consist of procedure status code and a description.
​
Execution steps:
1. get metadata info for documentation based on dataset text id;
2. download gdoc file as a pdf using google api;
3. upload the pdf to a dedicated S3 bucket.
​
​
## Dependencies
​
### Google service account payload
Access to the Google account is carried out using a secret. It is secured by AWS Secret Manager and can be taken using a `get_secret()` function. This value is then accepted as an argument in `service_account.Credentials.from_service_account_info()` to create credentials. They are needed to create a `service` object that will display the authorized user.
​
​
### Lambda layer
1. Google (`google`) - general Google library. Needed to perform authorization via `service_account` method imported from `google.oauth2`.
2. Google API client library for python docs (`googleapiclient`) - offers simple, flexible access to many Google APIs.
3. Requests (`requests`) - allows us to send HTTP/1.1 requests extremely easily.
​
### Metadata API
Credentials were set as environment variables:
metadata_api_name: service-as-data-arch-builder
metadata_api_secret: ...
​
​
## Deployment
​
**AWS Account:** algoseek-docs (729975684361)
​
**Secret Manager:** arn:aws:secretsmanager:us-east-1:729975684361:secret:aws-lambda-gdoc-credentials-a5xBra
​
**Lambda function:** arn:aws:lambda:us-east-1:729975684361:function:convert-gdoc-to-pdf
​
## Invokation
​
Example event
​
```
{
    "dataset_text_ids": ["eq_taq", "does_not_exist"]
}
```
​
Example response
```
[
    {
        "status_code": 200,
        "message": "OK"
    },
    {
        "status_code": 404,
        "message": "A record for Dataset is not found"
    }
]
```
​
​
## Possible error codes
​
|  Error Code   |                    Message                     |
| ------------- |:----------------------------------------------:|
|      200      | OK                                             |
|      404      | Dataset documentation does not exist           |
|      404      | A record for Dataset is not found              |
|      422      | Documentation source path is not set           |
|      422      | Documentation S3 destination is not set        |
|      422      | The source gdoc path is not properly formatted |
|      500      | Exception                                      |

​​
## Error reporting with SNS
