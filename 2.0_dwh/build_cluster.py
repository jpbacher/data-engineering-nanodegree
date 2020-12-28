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


def build_cluster(redshift, role_arn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES,
                  DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    # make Redshift cluster
    try:
        response = redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=DWH_NUM_NODES,
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUserame=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[role_arn]
        )
    except Exception as e:
        print(e)
