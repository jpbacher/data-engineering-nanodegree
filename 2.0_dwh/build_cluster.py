import configparser
import json
import pandas as pd
import psycopg2
import boto3
from botocore.exceptions import ClientError


def make_iam_role(iam, DWH_IAM_ROLE_NAME):
    # make an IAM role for Redshift
    try:
        print('Creating new IAM Role...')
        dwh_role = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description='Permits Redshift cluster to call AWS services',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts.AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'}
            )
        )
    except Exception as e:
        print(e)

    print('Attaching policy...')
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )['ResponseMetadata']['HTTPStatusCode']

    print('Get IAM Role ARN...')
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(f'role_arn: {role_arn}')
    return role_arn



