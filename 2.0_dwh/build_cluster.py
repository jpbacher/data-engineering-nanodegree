import configparser
import json
import pandas as pd
import psycopg2
import boto3
from botocore.exceptions import ClientError


def make_iam_role(iam, iam_role_name):
    # make an IAM role for Redshift
    try:
        print('Creating new IAM Role...')
        dwh_role = iam.create_role(
            Path='/',
            RoleName=iam_role_name,
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
        RoleName=iam_role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )['ResponseMetadata']['HTTPStatusCode']

    print('Get IAM Role ARN...')
    role_arn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']
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


def open_ports(ec2, cluster_props, dwh_port):
    # update security group to allow access
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        default_sec_grps = list(vpc.security_groups.all())[0]
        print(f'Security groups: {default_sec_grps}')
        default_sec_grps.authorize_ingress(
            GroupName=default_sec_grps.group_name,
            CirIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(dwh_port),
            ToPort=int(dwh_port)
        )
    except Exception as e:
        print(e)


