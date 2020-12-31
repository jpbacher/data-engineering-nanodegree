import configparser
import json
import pandas as pd
import psycopg2
import boto3
from botocore.exceptions import ClientError


def make_iam_role(iam, iam_role_name):
    # make an IAM role to grant permission to Redshift cluster
    try:
        print('Creating new IAM Role...')
        # must create IAM role in console
        dwh_role = iam.create_role(
            Path='/',
            RoleName=iam_role_name,
            Description='Allows Redshift clusters to call AWS services on your behalf',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts.AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'}
            )
        )
    except ClientError as e:
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


def build_cluster(redshift, role_arn, dwh_cluster_type, dwh_node_type, dwh_num_nodes,
                  dwh_db, dwh_cluster_identifier, dwh_db_user, dwh_db_password):
    # make Redshift cluster
    try:
        response = redshift.create_cluster(
            # hardware parameters
            ClusterType=dwh_cluster_type,
            NodeType=dwh_node_type,
            NumberOfNodes=dwh_num_nodes,
            # identifiers & credentials
            DBName=dwh_db,
            ClusterIdentifier=dwh_cluster_identifier,
            MasterUserame=dwh_db_user,
            MasterUserPassword=dwh_db_password,
            # roles for S3 access
            IamRoles=[role_arn]
        )
    except ClientError as e:
        print(e)


def get_cluster_properties(redshift, dwh_cluster_identifier):
    # get Redshift cluster properties
    cluster_props = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identifier)['Clusters'][0]
    display_redshift_props(cluster_props)

    dwh_endpoint = cluster_props['Endpoint']['Address']
    dwh_role_arn = cluster_props['IamRoles'][0]['IamRoleArn']
    print(f'DWH_ENDPOINT:: {dwh_endpoint}')  # used for host
    print(f'DWH_ROLE_ARN:: {dwh_role_arn}')
    return cluster_props, dwh_endpoint


def display_redshift_props(properties):
    # display cluster properties, check status is 'Available'
    pd.set_option('display.max_colwidth', None)
    prop_keys = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUserName', 'DBName',
                 'Endpoint', 'VpcId', 'NumberOfNodes']
    data = [(key, value) for key, value in properties.items() if key in prop_keys]
    cluster_df = pd.DataFrame(data=data, columns=['ClusterProps', 'Value'])
    return cluster_df


def open_ports(ec2, cluster_props, dwh_port):
    # open TCP port to access cluster endpoint
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        default_sec_grps = list(vpc.security_groups.all())[0]
        print(f'Security groups: {default_sec_grps}')
        default_sec_grps.authorize_ingress(
            GroupName=default_sec_grps.group_name,
            CirIp='0.0.0.0/0',  # not recommended - allows access from any computer
            IpProtocol='TCP',
            FromPort=int(dwh_port),
            ToPort=int(dwh_port)
        )
    except ClientError as e:
        print(e)


def delete_cluster(redshift, dwh_cluster_identifier, skip_snapshot=True):
    # delete cluster
    redshift.delete_cluster(ClusterIdentifier=dwh_cluster_identifier,
                            SkipFinalClusterSnapshot=skip_snapshot)


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

    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')

    cluster_df = pd.DataFrame(
        dict(Param=['DWH_CLUSTER_TYPE', 'DWH_NUM_NODES', 'DWH_NODE_TYPE',
                    'DWH_CLUSTER_IDENTIFIER', 'DWH_DB', 'DWH_DB_USER',
                    'DWH_PORT', 'DWH_IAM_ROLE_NAME'],
             Value=[DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE,
                    DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER,
                    DWH_PORT, DWH_IAM_ROLE_NAME]
             )
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
    iam = boto3.client('iam',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    role_arn = make_iam_role(iam, DWH_IAM_ROLE_NAME)

    build_cluster(redshift, role_arn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES,
                  DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)

    cluster_properties, dwh_endpoint = get_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER)

    open_ports(ec2, cluster_properties, DWH_PORT)

    conn = psycopg2.connect('host={} dbname={} user={} password={} port={}'.format(
        dwh_endpoint, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)
    )
    cur = conn.cursor()
    print('Connected...')

    conn.close()


if __name__ == '__main__':
    main()
