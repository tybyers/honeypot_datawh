import pandas as pd
import boto3
import json
import configparser

class redshift(object):

    def __init__(self, config_file='aws.cfg'):

        config = configparser.ConfigParser()
        with open(config_file) as cf:
            config.read_file(cf)
        self.KEY = config.get('AWS', 'KEY')
        self.SECRET = config.get('AWS', 'SECRET')

        self.DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
        self.DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
        self.DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')
        self.DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
        self.DWH_DB = config.get('DWH', 'DWH_DB')
        self.DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
        self.DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
        self.DWH_PORT = config.get('DWH', 'DWH_PORT')
        self.DWH_ENDPOINT = ''

        self.redshiftdb = boto3.client('redshift', 
                                    region_name='us-west-2',
                                    aws_access_key_id=self.KEY,
                                    aws_secret_access_key=self.SECRET)

        
    def create_cluster(self, verbose=True):

        try:
            response = self.redshiftdb.create_cluster(
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),

                DBName=self.DWH_DB,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD

                # IamRole for s3 access if needed
                #IamRoles=[roleArn]
            )
            if verbose: print('Creating cluster. Please run "get_cluster_info" to check status.')
        except Exception as e:
            print(e)

    def get_cluster_info(self):

        cluster_info = self.redshiftdb.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        show_keys = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUsername', 'DBName',\
                    'Endpoint', 'NumberOfNodes', 'VpcId']
        cluster_info = {k: v for k,v in cluster_info.items() if k in show_keys}

        if 'Endpoint' in cluster_info:
            self.DWH_ENDPOINT = cluster_info['Endpoint']['Address']
        else:
            print('Cluster is still building. Please check back.')

        return cluster_info

    def test_cluster_connection(self):

        conn_str = "postgresql://{}:{}@{}:{}/{}".format(
            self.DWH_DB_USER,
            self.DWH_DB_PASSWORD,
            self.DWH_ENDPOINT,
            self.DWH_PORT,
            self.DWH_DB
        )

        return conn_str

    def shutdown_cluster(self):

        self.redshiftdb.delete_cluster(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot = True)