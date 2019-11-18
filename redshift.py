import pandas as pd
import boto3
import json
import configparser

class redshift(object):
    """ This class allows a user to create a Redshift cluster using information entered into a 
    configuration file. The usser must have already set up a Key and Secret pair on AWS. Users may 
    also use methods within to delete the cluster once they are done. If a cluster is brought up
    with this process, it may still be accessed using this class even if the Python kernel is restarted,
    since the Redshift cluster will persist until explicitly deleted. 

    Parameters
    ----------
    config_file: str
      Path to the configuration file, which may be read by the `configparser` package. A sample 
      configuration parse, "aws_example.cfg", should be distributed with this package. 

    """

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
        self.IAM_ROLE_NAME = config.get('DWH', 'DWH_IAM_ROLE_NAME')
        self.IAM_ROLE = ''

        self.redshiftdb = boto3.client('redshift', 
                                    region_name='us-west-2',
                                    aws_access_key_id=self.KEY,
                                    aws_secret_access_key=self.SECRET)

        self.iam = boto3.client('iam', region_name='us-west-2',
                                    aws_access_key_id=self.KEY,
                                    aws_secret_access_key=self.SECRET)
        
    def create_cluster(self, verbose=True):
        """ Create a redshift cluster. This method creates a new Redshift cluster using the 
        parameters specified in the config file. However, the user may also change the cluster
        parameters on the fly using the various `DWH_*` attributes of the redshift object.

        Parameters
        ----------
        verbose: bool, default True
          Prints out a status message when the cluster is being created.

        Returns
        -------
        None. The cluster db object will reside in the `redshiftdb` attribute of the redshift object. 
        """
        # attach IAM role/policy first
        self.attach_iam_role()

        try:
            response = self.redshiftdb.create_cluster(
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),

                DBName=self.DWH_DB,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,

                # IamRole for s3 access if needed
                IamRoles=[self.IAM_ROLE]
            )
            if verbose: print('Creating cluster. Please run "get_cluster_info" to check status.')
        except Exception as e:
            print(e)

    def get_cluster_info(self):
        """ Print out the information about the cluster. This is especially important when the cluster
        is being created or deleted. This function also acts as an important function to run *after* the
        cluster has been created, so that the Endpoint information can be added to the `DWH_ENDPOINT` attribute
        of the redshift object. This is important if you need to read/write access to the cluster.

        Parameters
        ----------
        None

        Returns
        -------
        dict of important cluster information.
        """

        cluster_info = self.redshiftdb.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        show_keys = ['ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUsername', 'DBName',\
                    'Endpoint', 'NumberOfNodes', 'VpcId']
        cluster_info = {k: v for k,v in cluster_info.items() if k in show_keys}

        if 'Endpoint' in cluster_info:
            if isinstance(cluster_info['Endpoint'], dict) and 'Address' in cluster_info['Endpoint']:
                self.DWH_ENDPOINT = cluster_info['Endpoint']['Address']
                self.IAM_ROLE = self.iam.get_role(RoleName=self.IAM_ROLE_NAME)['Role']['Arn']
            else:
                print('Cluster may still be building or deleting. Please check back.')
        else:
            print('Cluster is still building. Please check back.')

        return cluster_info

    def attach_iam_role(self):
        """ Create a new IAM role and associate it IAM role with the database. If an IAM role with the
        name already exists, it will still attach the role to the database. 

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        try:
            print('Creating a new IAM Role')
            dwhrole = self.iam.create_role(
                Path='/',
                RoleName=self.IAM_ROLE_NAME,
                Description= 'Allows Redshift clusters to call AWS services on your behalf.',
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'}
                )
            )
        except Exception as e:
            print(e)

        # attach policy
        print('Attaching IAM policy')
        self.iam.attach_role_policy(RoleName=self.IAM_ROLE_NAME,
                                    PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
        # get IAM role
        self.IAM_ROLE = self.iam.get_role(RoleName=self.IAM_ROLE_NAME)['Role']['Arn']


    def test_cluster_connection(self):
        """ This method can be used to test connection to the cluster. To verify connection worked, you
        will need to do something like the following in jupyter notebooks:

        Example
        -------
        %load_ext sql
        rs = redshift()
        rs.db_connect()
        cs = rs.test_cluster_connection()
        %sql $cs

        # output may look like the following:
        'Connected: honeypotuser@honeypot'

        Parameters
        ----------
        None

        Returns
        -------
        str, connection to database.
        """

        conn_str = "postgresql://{}:{}@{}:{}/{}".format(
            self.DWH_DB_USER,
            self.DWH_DB_PASSWORD,
            self.DWH_ENDPOINT,
            self.DWH_PORT,
            self.DWH_DB
        )

        return conn_str

    def shutdown_cluster(self):
        """ Shutdown the cluster stored in the `redshiftdb` attribute of the redshift object.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        self.redshiftdb.delete_cluster(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot = True)