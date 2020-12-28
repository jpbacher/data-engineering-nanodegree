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


def build_cluster(redshift, role_arn, cluster_type, node_type, num_nodes,
                  dwh_db, cluster_identifier, db_user, db_user_password):
    # make Redshift cluster
    try:
        response = redshift.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=num_nodes,
            DBName=dwh_db,
            ClusterIdentifier=cluster_identifier,
            MasterUserame=db_user,
            MasterUserPassword=db_user_password,
            IamRoles=[role_arn]
        )
    except Exception as e:
        print(e)


def get_cluster_properties(redshift, cluster_identifier):
    # get Redshift cluster properties
    cluster_props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    dwh_endpoint = cluster_props['Endpoint']['Address']
    dwh_role_arn = cluster_props['IamRoles'][0]['IamRoleArn']
    print(f'DWH_ENDPOINT: {dwh_endpoint}')
    print(f'DWH_ROLE_ARN: {dwh_role_arn}')
    return cluster_props, dwh_endpoint, dwh_role_arn


