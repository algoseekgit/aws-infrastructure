from typing import Dict, Any, List
import urllib
import json
import copy
import boto3
import math
import os


def request(
        url: str,
        params: Dict[str, Any] = {}
) -> Dict[str, Any]:
    """
    Sends Get request to the Metadata API.

    :param url: The endpoint URL
    :param params: Optional argument to add parameters to the endpoint URL
    return: Returns a dictionary if the response status code is 200 otherwise raise an Exception.
    """
    api_login_name = os.getenv('API_LOGIN_NAME')  # 'Maintainer'
    api_login_secret = os.getenv('API_LOGIN_SECRET')  # 'CyDMEmLotWsykvOo'
    api_server = os.getenv('API_SERVER')  # 'metadata-services.algoseek.com'
    api_prefix = os.getenv('API_PREFIX')  # 'api/v1'

    base_url = f'https://{api_server}/{api_prefix}'
    token_url = 'login/access_token/'

    # Prepare data to send in request body to get the token
    login_data = {
        'name': api_login_name,
        'secret': api_login_secret
    }
    login_data = json.dumps(login_data).encode("utf-8")
    try:
        # Send a POST request to get the token
        req = urllib.request.Request(f"{base_url}/{token_url}", login_data)
        req.add_header("Content-Type", "application/json")
        response = urllib.request.urlopen(req)
        token_data = json.loads(response.read().decode())
        access_token = token_data['token']
        # Use the access token to send a GET request to another API endpoint
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        params_string = "&".join([f"{key}={value}" for key, value in params.items()])
        full_url = f"{base_url}/{url.lstrip('/')}?{params_string}" if params_string else f"{base_url}/{url.lstrip('/')}"
        req = urllib.request.Request(full_url, headers=headers)
        response = urllib.request.urlopen(req)
        # Checking status code of response
        if response.status != 200:
            raise RuntimeError("Couldn't reach the endpoint: {full_url}")
        # Read the response and decode it as JSON
        return json.loads(response.read().decode())

    except urllib.error.HTTPError as e:
        print(f'HTTP error occurred: {e.code} - {e.reason}')
        raise RuntimeError(f'HTTP error occurred: {e.code} - {e.reason}')

    except urllib.error.URLError as e:
        print(f'URL error occurred: {e.reason}')
        raise RuntimeError(f'URL error occurred: {e.reason}')


def get_bkgs_to_monitor() -> Dict[str, Dict[str, Any]]:
    """
    Lists all active bucketgroups and related models

    return: Returns the dictionary of all active bucketgroups and relative details.
    """
    default_timeout_minutes = 180
    default_delay_alert_minutes = 60
    # List all bucketgroups and parent models
    bkg_url = 'internal/bucket_group/'
    bucketgroups = request(
        bkg_url,
        params={"is_active": True}
    )
    update_url = 'internal/bucket_update/'
    updates = request(
        update_url
    )
    updates = {record['id']: record for record in updates}
    monitor_url = 'internal/bucket_monitoring/'
    monitors = request(
        monitor_url
    )
    monitors = {record['id']: record for record in monitors}
    cloud_storage_url = 'internal/cloud_storage/'
    cloud_storages = request(
        cloud_storage_url
    )
    cloud_storages = {record['id']: record for record in cloud_storages}
    datasets_url = 'internal/dataset/'
    datasets = request(
        datasets_url
    )
    datasets = {record['id']: record for record in datasets}
    data_class_url = 'internal/data_class/'
    data_classes = request(
        data_class_url
    )
    data_classes = {record['id']: record for record in data_classes}

    # Filter out bucket group with active updates
    bucketgroups_to_monitor = dict()
    for bkg in bucketgroups:
        bkg_updates_id = bkg.get("updates_id")
        if bkg_updates_id:
            bkg_updates = updates[bkg_updates_id]
            if bkg_updates['is_active'] and bkg_updates['expected_time']: #TODO expected time should be set for each active updates
                bkg_monitor_id = bkg.get("monitoring_id")
                if bkg_monitor_id:
                    monitor_details = monitors[bkg_monitor_id]
                    bkg_updates['delay_alert_minutes'] = monitor_details['delay_alert_minutes']
                    bkg_updates['timeout_minutes'] = monitor_details['timeout_minutes']
                else:
                    bkg_updates['delay_alert_minutes'] = default_delay_alert_minutes
                    bkg_updates['timeout_minutes'] = default_timeout_minutes
                bkg_updates['bucket_name'] = bkg['bucket_name']
                bkg_details = copy.deepcopy(bkg)
                bkg_details['bucket_updates'] = bkg_updates
                cloud_storage = cloud_storages[bkg['cloud_storage_id']]
                dataset = datasets[cloud_storage['dataset_id']]
                data_class = data_classes[dataset['data_class_id']]
                bkg_details['data_class_text_id'] = data_class['text_id']
                bucketgroups_to_monitor[bkg_details['text_id']] = bkg_details
    return bucketgroups_to_monitor


def generate_crontab_expression(bkg_details: Dict[str, Any]) -> str:
    """
    Generates crontab expression for checking the existence of daily updates per bucketgroup

    :param bkg_details: The bucketgroup details
    return: Returns a crontab expression.
    """
    # Dictionary to define week range of checks based on data class
    days_of_week = {
        'Futures': {
            0: 'SUN-FRI',
            1: 'MON-SAT',
            2: 'TUE-SUN'
        },
        'Equity': {
            0: 'MON-FRI',
            1: 'TUE-SAT',
            2: 'WED-SUN'
        }
    }
    bkg_updates = bkg_details['bucket_updates']
    exp_hour, exp_min, exp_sec = bkg_updates['expected_time'].split(':')
    if bkg_details['data_class_text_id'] in ('fu', 'fo'):
        week_days = days_of_week['Futures'][bkg_updates['days_offset']]
    else:
        week_days = days_of_week['Equity'][bkg_updates['days_offset']]
    # Need to check updates periodiclly during the day
    if bkg_updates.get('intraday_period_minutes'):
        # crontab example cron(0 3-23/2 * * ? *)
        period = math.ceil(bkg_updates['intraday_period_minutes']/60)
        expression = f"cron({int(exp_min)} {int(exp_hour)}-23/{period} ? * {week_days} *)"
    # Need to check updates at specific time
    else:
        # crontab example cron(0 5,6,7 * * ? *)
        alert_hour = int(exp_hour) + math.ceil(bkg_updates['delay_alert_minutes']/60)
        failure_hour = int(exp_hour) + math.ceil(bkg_updates['timeout_minutes']/60)
        hour_to_check_alert = min(alert_hour, 23)
        hour_to_check_failure = min(failure_hour, 23)

        expression = f"cron({int(exp_min)} {int(exp_hour)},{hour_to_check_alert},{hour_to_check_failure} ? * {week_days} *)"
    return expression


#List all existing Event Bridge rules
def list_scheduler_rules(
    scheduler
) -> List[Dict[str, Any]]:
    """
    Lists a Scheduler Rules on the AWS side

    :return: Returns the list of Scheduler Rules
    """
    event_rules = []
    response = scheduler.list_schedules(
        NamePrefix='bkg_update_monitor_',
        MaxResults=100
    )
    event_rules.extend(response['Schedules'])
    next_token = response.get('NextToken')
    while next_token:
        response = scheduler.list_schedules(
            NextToken=next_token,
            NamePrefix='bkg_update_monitor_',
            MaxResults=100
        )
        event_rules.extend(response['Schedules'])
        next_token = response.get('NextToken')
    return event_rules


def create_scheduler(
    scheduler,
    bkg_text_id: str,
    crontab_expression: str,
    lambda_input: Dict[str, Any]
) -> Any:
    """
    Creates a Scheduler Rule on the AWS side

    :param bkg_text_id: The bucketgroup text id
    :param crontab_expression: The crontab expression for task scheduler
    :param lambda_input: The dictionary as input for the Lambda to be triggered
    """
    scheduler.create_schedule(
        Description=f'Daily updates monitoring of {bkg_text_id}',
        FlexibleTimeWindow={
            'Mode': 'OFF'
        },
        Name=f'bkg_update_monitor_{bkg_text_id}',
        ScheduleExpression=crontab_expression,
        ScheduleExpressionTimezone='America/New_York',
        Target={
            'Arn': os.getenv('LAMBDA_FUNCTION_ARN'),
            'Input': json.dumps(lambda_input),
            'RoleArn': os.getenv('LAMBDA_FUNCTION_ROLE_ARN'),
            'RetryPolicy': {
                'MaximumEventAgeInSeconds': 60,
                'MaximumRetryAttempts': 1
            }
        }
    )


def put_scheduler(
    scheduler,
    bkg_text_id: str,
    crontab_expression: str,
    lambda_input: Dict[str, Any]
) -> None:
    """
    updates a Scheduler Rule on the AWS side

    :param bkg_text_id: The bucketgroup text id
    :param crontab_expression: The crontab expression for task scheduler
    :param lambda_input: The dictionary as input for the Lambda to be triggered
    """
    scheduler.update_schedule(
        Description=f'Daily updates monitoring of {bkg_text_id}',
        FlexibleTimeWindow={
            'Mode': 'OFF'
        },
        Name=f'bkg_update_monitor_{bkg_text_id}',
        ScheduleExpression=crontab_expression,
        ScheduleExpressionTimezone='America/New_York',
        Target={
            'Arn': os.getenv('LAMBDA_FUNCTION_ARN'),
            'Input': json.dumps(lambda_input),
            'RoleArn': os.getenv('LAMBDA_FUNCTION_ROLE_ARN'),
            'RetryPolicy': {
                'MaximumEventAgeInSeconds': 60,
                'MaximumRetryAttempts': 1
            }
        }
    )


def delete_scheduler(
    scheduler,
    bkg_text_id: str
) -> None:
    """
    Deletes a Scheduler Rule on the AWS side

    :param bkg_text_id: The bucketgroup text id
    """
    scheduler.delete_schedule(
        Name=f'bkg_update_monitor_{bkg_text_id}'
    )


def lambda_handler(event, context):
    session = boto3.Session(region_name='us-east-1')
    scheduler = session.client('scheduler')

    bkgs_to_monitor = get_bkgs_to_monitor()
    existng_event_bridge_rules = list_scheduler_rules(scheduler)   # List of dict
    existng_event_bridge_rules = {
        rule['Name'].replace('bkg_update_monitor_', ''): rule for rule in existng_event_bridge_rules
    }
    # Define bucketgroups for which scheduler should be added/updated/deleted
    event_bridge_rule_to_update = set(existng_event_bridge_rules) & set(bkgs_to_monitor)
    event_bridge_rule_to_add = set(bkgs_to_monitor) - set(existng_event_bridge_rules)
    event_bridge_rule_to_delete = set(existng_event_bridge_rules) - set(bkgs_to_monitor)

    for event in event_bridge_rule_to_add:
        expression = generate_crontab_expression(bkgs_to_monitor[event])
        lambda_input = {
            "bkg_text_id": event,
            "bkg_updates": bkgs_to_monitor[event]['bucket_updates']
        }
        create_scheduler(
            scheduler,
            event,
            expression,
            lambda_input
        )
    for event in event_bridge_rule_to_update:
        expression = generate_crontab_expression(bkgs_to_monitor[event])
        lambda_input = {
            "bkg_text_id": event,
            "bkg_updates": bkgs_to_monitor[event]['bucket_updates']
        }
        put_scheduler(
            scheduler,
            event,
            expression,
            lambda_input
        )
    for event in event_bridge_rule_to_delete:
        delete_scheduler(
            scheduler,
            event
        )
