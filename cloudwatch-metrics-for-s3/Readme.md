# Cloudwatch metrics for S3

## Introduction

This project is designed to collect cloudwatch metrics from S3 buckets in multiple AWS accounts and save the information in `.csv` file. 

It consists of two separate lambda functions. The main function, named `fetch-consolidated-cloudwatch-metrics-for-s3`, is responsible for the direct collection and storage of information from a dedicated AWS account. And the secondary one, named `invoke-consolidated-cloudwatch-metrics-for-s3`, provides iteration through the list of accounts and invokes the main function with a specific payload on each loop step. Let the secondary function be also named as invoker.

## Functionality

Invoker is triggered once a month at a predetermined date (the second day of the month). It takes a list of accounts as input and starts the iteration through them. Every time this function invokes the main function with a payload containing the name of AWS account. Any additional event attributes specified for invoker are passed to the main function as well.

The main function reads `account_name` from the incoming event and creates boto3 session for that AWS account. Credentials for account are stored as environment variables for the main function. Then the main function begins to collect cloudwatch metrics from all S3 buckets in the account and write it to a file. Finally, the file is uploaded to a dedicated S3 bucket.

### Use cases

#### Automatic usage

In general, event for invoker does not require any attributes and remains empty (`{}`). In this case only predefined list of accounts (`default_accounts`) which is set in the invoker function will be used.

#### Manual usage

Invoker function also can be triggered manually. In this case event must be specified.

1) If you want to collect cloudwatch metrics for a specific date in the past, you have to add the `date` key and its value in `%Y-%m-%d` format into invoker event.

2) If you want to collect cloudwatch metrics for a specific account or list of accounts, you have to add `account_name` key and a list of values into invoker event.

### Environment variables

Credentials for AWS accounts are stored as environment variables in `fetch-consolidated-cloudwatch-metrics-for-s3` lambda function. If you want to add account in the `default_accounts` or you want manually trigger function for specific accounts, you have to add dedicated credentials as environment variables. The expected format is as follows:

| Variable                            | Description                                  |
| ----------------------------------- | -------------------------------------------- |
| {`account_name`}_account_access_key | access key id for `account_name` account     |
| {`account_name`}_secret_access_key  | secret access key for `account_name` account |

Note: `account_name` in environment variable must be written with underscores '`_`' even if the real account name was written with dashes '`-`'. It is the only way to symbolically write the names. There is a piece of code inside the main function which replace dashes with underscores in `account_name` to match the environment variables' names.