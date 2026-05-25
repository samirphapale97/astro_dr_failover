import json
import boto3
import os
import urllib.request
from datetime import datetime, timezone


def handler(event, context):
    """
    Auto-failover Lambda.
    Triggered by CloudWatch Alarm via SNS when Route53 health check fails.
    1. Reads current active region from SSM
    2. Switches to DR region
    3. Sends Slack notification
    4. Optionally calls Astro API
    """
    ssm = boto3.client('ssm', region_name=os.environ['AWS_REGION'])
    param_name = os.environ['SSM_PARAMETER_NAME']
    primary_region = os.environ['PRIMARY_REGION']
    dr_region = os.environ['DR_REGION']
    slack_webhook = os.environ.get('SLACK_WEBHOOK_URL', '')
    astro_api_key = os.environ.get('ASTRO_API_KEY', '')
    astro_deployment_id = os.environ.get('ASTRO_DEPLOYMENT_ID', '')
    astro_api_url = os.environ.get('ASTRO_API_URL', 'https://api.astronomer.io')

    # Parse alarm state from SNS message
    sns_message = json.loads(event['Records'][0]['Sns']['Message'])
    alarm_state = sns_message.get('NewStateValue', 'UNKNOWN')
    alarm_reason = sns_message.get('NewStateReason', 'No reason provided')

    # Get current active region
    current = ssm.get_parameter(Name=param_name)['Parameter']['Value']

    if alarm_state == 'ALARM':
        # Primary is down → switch to DR
        target = dr_region if current == primary_region else primary_region
        ssm.put_parameter(
            Name=param_name, Value=target, Type='String', Overwrite=True
        )

        # Update Astro deployment env vars if configured
        if astro_api_key and astro_deployment_id:
            try:
                update_astro_env(
                    astro_api_url, astro_api_key, astro_deployment_id, target
                )
            except Exception as e:
                print(f'Astro API update failed: {e}')

        # Send Slack notification
        if slack_webhook:
            send_slack_notification(slack_webhook, current, target, alarm_reason)

        return {
            'statusCode': 200,
            'body': json.dumps(
                {'from': current, 'to': target, 'status': 'FAILOVER_COMPLETE'}
            ),
        }

    elif alarm_state == 'OK':
        # Region recovered — send recovery notification but DON'T auto-failback
        if slack_webhook:
            send_slack_recovery(slack_webhook, current)
        return {
            'statusCode': 200,
            'body': json.dumps(
                {'status': 'RECOVERY_DETECTED', 'active_region': current}
            ),
        }

    return {
        'statusCode': 200,
        'body': json.dumps({'status': 'NO_ACTION', 'alarm_state': alarm_state}),
    }


def update_astro_env(api_url, api_key, deployment_id, region):
    """Update Astro deployment environment variables with the new active region."""
    url = f'{api_url}/v1/deployments/{deployment_id}/variables'
    data = json.dumps(
        [{'key': 'ACTIVE_REGION', 'value': region, 'isSecret': False}]
    ).encode()
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Authorization', f'Bearer {api_key}')
    req.add_header('Content-Type', 'application/json')
    urllib.request.urlopen(req, timeout=10)


def send_slack_notification(webhook_url, from_region, to_region, reason):
    """Send a Slack notification for a DR failover event."""
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    payload = {
        'blocks': [
            {
                'type': 'header',
                'text': {
                    'type': 'plain_text',
                    'text': '🚨 DR FAILOVER TRIGGERED',
                    'emoji': True,
                },
            },
            {
                'type': 'section',
                'fields': [
                    {
                        'type': 'mrkdwn',
                        'text': f'*From Region:*\n`{from_region}`',
                    },
                    {
                        'type': 'mrkdwn',
                        'text': f'*To Region:*\n`{to_region}`',
                    },
                    {'type': 'mrkdwn', 'text': f'*Timestamp:*\n{now}'},
                    {
                        'type': 'mrkdwn',
                        'text': '*Trigger:*\nAutomatic (Health Check Failed)',
                    },
                ],
            },
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': f'*Reason:*\n```{reason}```',
                },
            },
            {
                'type': 'context',
                'elements': [
                    {
                        'type': 'mrkdwn',
                        'text': '🤖 Automated by Astro DR Failover Lambda',
                    }
                ],
            },
        ]
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(webhook_url, data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f'Slack notification failed: {e}')


def send_slack_recovery(webhook_url, current_region):
    """Send a Slack notification when region recovery is detected."""
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    payload = {
        'blocks': [
            {
                'type': 'header',
                'text': {
                    'type': 'plain_text',
                    'text': '✅ Region Recovery Detected',
                    'emoji': True,
                },
            },
            {
                'type': 'section',
                'fields': [
                    {
                        'type': 'mrkdwn',
                        'text': f'*Active Region:*\n`{current_region}`',
                    },
                    {'type': 'mrkdwn', 'text': f'*Timestamp:*\n{now}'},
                ],
            },
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': '⚠️ *Auto-failback is disabled.* Use the manual failover DAG to switch back if needed.',
                },
            },
            {
                'type': 'context',
                'elements': [
                    {
                        'type': 'mrkdwn',
                        'text': '🤖 Automated by Astro DR Failover Lambda',
                    }
                ],
            },
        ]
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(webhook_url, data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f'Slack notification failed: {e}')
