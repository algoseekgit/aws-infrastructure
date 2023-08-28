import json
import boto3
from datetime import datetime, timedelta
import math
import os
from typing import Any
import dateutil.tz


alert_message = '''
Hello,

The update for bucketgroup __bkg_text_id__ (trading date __trade_date__) is delayed because of technical issues.

This affects the following buckets:
__bucket_details__

Algoseek Team
'''

failure_message = '''
Hello,

For technical reasons, the bucketgroup __bkg_text_id__ (trading date __trade_date__) has not been updated.

This affects the following buckets:
__bucket_details__

Algoseek Team
'''


def send_sns_alert(sns_client, message: str) -> None:
    """
    Send a message to the SNS topic on alert/failure

    :param message: The error message to sent
    """
    sns_client.publish(
        TopicArn=os.getenv('SNS_TOPIC_ARN'),
        Message=message,
        Subject="Daily Updates Monitoring System",
    )


def get_dynamo_db_record(dynamodb_table, bkg_id: str, tradedate: str) -> Any:
    """
    Get a record of daily updates from the DynamoDB table

    :param bkg_id: The bucketgroup text id
    :param tradedate: The trading date in yyyymmdd format to get updates record
    """
    lookup_key = {
        'bucketgroup_text_id': bkg_id,
        'tradedate': tradedate
    }
    item = dynamodb_table.get_item(Key=lookup_key).get('Item')
    return item


def get_start_interval_to_check(days_offset: int, intraday_period_minutes: int) -> datetime:
    """
    Get the start datetime for checking updates.
    This function supports bucket groups with intraday periodical updates

    :param days_offset: The offset in days to check updates
    :param intraday_period_minutes: The period to check updates in minutes
    """
    utcnow = datetime.utcnow()
    start_interval = utcnow - timedelta(days=days_offset, hours=math.ceil(intraday_period_minutes/60))
    return start_interval


def lambda_handler(event, context):
    bkg_text_id = event['bkg_text_id']
    bkg_updates = event['bkg_updates']
    expected_time = bkg_updates['expected_time']
    timeout_minutes = bkg_updates['timeout_minutes']
    bucket_name = bkg_updates['bucket_name']

    session = boto3.Session(region_name='us-east-1',)
    dynamodb = session.resource('dynamodb')
    sns = session.client("sns")
    dynamodb_table = dynamodb.Table(os.getenv('TABLE_NAME'))

    # Get current time in EDT timezone
    edt_timezone = dateutil.tz.gettz('America/New_York')
    current_time_edt = datetime.now(tz=edt_timezone)

    # trading day to check updates
    trading_day = (current_time_edt - timedelta(days=bkg_updates['days_offset'])).date().strftime('%Y%m%d')

    # Generate alert/failure messages
    failure_msg = failure_message.replace(
        '__bkg_text_id__', bkg_text_id
    ).replace(
        '__trade_date__', trading_day
    ).replace('__bucket_details__', bucket_name)

    alert_msg = alert_message.replace(
        '__bkg_text_id__', bkg_text_id
    ).replace(
        '__trade_date__', trading_day
    ).replace('__bucket_details__', bucket_name)


    if bkg_updates['intraday_period_minutes']:
        start_interval = get_start_interval_to_check(
            bkg_updates['days_offset'],
            bkg_updates['intraday_period_minutes']
        )
        # Get record from dynamo db
        db_record = get_dynamo_db_record(
            dynamodb_table,
            bkg_text_id,
            trading_day
        )
        event_log = json.loads(db_record.get('events_log', "[]"))
        update_times = [record['modified'] for record in event_log]
        update_times.append(db_record['modified'])
        # Checking existence of updates
        for update_time in update_times:
            update_time_dt = datetime.strptime('%Y-%m-%dT%H:%M:%S.%fZ', update_time)
            if update_time_dt >= start_interval:
                return
        send_sns_alert(
            sns,
            failure_msg
        )
    else:
        # Get record from dynamo db
        db_record = get_dynamo_db_record(
            dynamodb_table,
            bkg_text_id,
            trading_day
        )
        if not db_record:
            # Define notification type alert/failure
            exp_hour, exp_min, exp_sec = expected_time.split(':')
            exp_timeout_hour_ = int(exp_hour) + math.ceil(timeout_minutes/60)
            exp_timeout_hour = exp_timeout_hour_ if exp_timeout_hour_ <= 23 else 23
            if current_time_edt.hour >= exp_timeout_hour:
                send_sns_alert(
                    sns,
                    failure_msg
                )
            else:
                send_sns_alert(
                    sns,
                    alert_msg
                )
