##### 1. Run AWS CloudFormation and use CloudFormation_SNS_Lambda_Slack.yaml with further default parameters to create lambda layer (slack-package-python39) and Lambda function (SNS-to-slack-lambda)
##### 2. Edit environmental variable in configuration of the lambda function using proper Slack API token.
##### 3. For test execution choose standard SNS notification. Overwrite TopicArn with the one containing proper chanel ("TopicArn": "arn:aws:sns:us-east-1:123456789012:dev-data-lake").
