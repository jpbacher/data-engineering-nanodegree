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


def main():

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
    DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')

    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    DWH_DB = config.get('DWH', 'DWH_DB')
    DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
    DWH_PORT = config.get('DWH', 'DWH_PORT')

    DWH_IAM_ROLE_NAME = config.get('DWH', 'DWH_IAM_ROLE_NAME')

    cluster_df = pd.DataFrame(
        {'Param': ['DWH_CLUSTER_TYPE', 'DWH_NUM_NODES', 'DWH_NODE_TYPE',
                   'DWH_CLUSTER_IDENTIFIER', 'DWH_DB'],
            'Value': [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE,
                      DWH_CLUSTER_IDENTIFIER, DWH_DB]
         }
    )
    print(cluster_df)

    ec2 = boto3.resource('ec2',
                         region_name='us-west-2',
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)
    s3 = boto3.resource('s3',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
    iam = boto3.cliet('iam',
                      region_name='us-west-2',
                      aws_access_key_id=KEY,
                      aws_secret_access_key=SECRET)


