import pandas as pd
import boto3
from redshift import redshift
import configparser
import psycopg2
from sql_queries import honeypot_sql as SQL_QUERIES

class honeypot_redshift(redshift):
    """ This class extends the `redshift` class and allows the user to set up the honeypot data warehouse in redshift.
    Please see documentation for the `redshift` class for instructions about setting up the cluster. This class then 
    gives the user the ability to drop, create, copy, and insert into the proper tables in the honeypot data warehouse.
    It uses SQL queries in the `sql_queries` directory that should come with this package.

    Parameters
    ----------
    config_file: str
      Path to the configuration file, which may be read by the `configparser` package. A sample 
      configuration parse, "aws_example.cfg", should be distributed with this package. 
    """

    def __init__(self, config_file='aws.cfg'):
        super().__init__(config_file)

        config = configparser.ConfigParser()
        with open(config_file) as cf:
            config.read_file(cf)
        self.s3_honeypot = config.get('S3', 'HONEYPOT_DATA')
        self.s3_reputation = config.get('S3', 'REPUTATION_DATA')
        self.s3_ipgeo = config.get('S3', 'IP_GEO_DATA')
        self.table_cmds = SQL_QUERIES.table_commands # all sql commands for the tables
        self.conn = None # db connection
        self.data_paths = {
            'staging_honeypot': self.s3_honeypot,
            'staging_reputation': self.s3_reputation,
            'staging_ipgeo': self.s3_ipgeo
        }

    def db_connect(self):
        """ Connect to the redshift database and estabish a psycogp2 connection object.

        Parameters
        ----------
        None

        Returns
        -------
        None. The database connection object is stored in the `conn` attribute of the honeypot_redshift object.
        """

        self.get_cluster_info()
        self.conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".\
            format(self.DWH_ENDPOINT, self.DWH_DB, self.DWH_DB_USER, self.DWH_DB_PASSWORD, self.DWH_PORT))
    

    def delete_tables(self, tables='all'):
        """ Delete all or some of the tables in the database.

        Parameters
        ----------
        tables: str or list, default "all"
          If the user wishes to delete all the tables to start clean, leave this parameter as the default
          "all". However, the user may pass a list of the exact table names to delete, if desired.

        Returns
        -------
        None
        """

        cur = self.conn.cursor()

        if tables == 'all':
            table_names = self.table_cmds.keys()
        elif isinstance(tables, list):
            table_names = tables
        else:
            raise ValueError('`tables` parameter must be "all" or of type list.')

        for table in table_names:
            print('Dropping table: {}'.format(table))
            cur.execute(self.table_cmds[table]['drop'])
            self.conn.commit()

    def create_tables(self, tables='all'):
        """ Create all or some of the tables in the database. The user may see the SQL queries for 
        creating the tables by inspecting the `table_cmds` attribute of the `honeypot_redshift` object.

        Parameters
        ----------
        tables: str or list, default "all"
          If the user wishes to create all the tables as specified in the sql_queries code,
           leave this parameter as the default "all". However, the user may pass a list of the exact 
           table names to create, if desired. 

        Returns
        -------
        None
        """

        cur = self.conn.cursor()

        if tables == 'all':
            table_names = self.table_cmds.keys()
        elif isinstance(tables, list):
            table_names = tables
        else:
            raise ValueError('`tables` parameter must be "all" or of type list.')

        for table in table_names:
            print('Creating table: {}'.format(table))
            cur.execute(self.table_cmds[table]['create']) 
            self.conn.commit()

    def copy_into_tables(self, tables='all'):
        """ Copy data from S3 into the staging tables. 

        Parameters
        ----------
        tables: str or list, default "all"
          If the user wishes to copy data to all the staging tables as specified in the sql_queries code,
           leave this parameter as the default "all". However, the user may pass a list of the exact 
           table names to copy to, if desired. 

        Returns
        -------
        None
        """
        cur = self.conn.cursor()

        if tables == 'all':
            table_names = self.table_cmds.keys()
        elif isinstance(tables, list):
            table_names = tables
        else:
            raise ValueError('`tables` parameter must be "all" or of type list.')

        for table in table_names:
            if 'copy' in self.table_cmds[table]:
                print('Copying into table: {}'.format(table))
                cmd = self.table_cmds[table]['copy'].format(
                    self.data_paths[table], self.IAM_ROLE
                )
                cur.execute(cmd) 
                self.conn.commit()

    def insert_into_tables(self, tables='all'):
        """ Insert data from the S3 tables into the fact and dimension tables. 

        Parameters
        ----------
        tables: str or list, default "all"
          If the user wishes to insert data to all the fact/dim tables as specified in the sql_queries code,
           leave this parameter as the default "all". However, the user may pass a list of the exact 
           table names to insert to, if desired. 

        Returns
        -------
        None
        """

        cur = self.conn.cursor()

        if tables == 'all':
            table_names = self.table_cmds.keys()
        elif isinstance(tables, list):
            table_names = tables
        else:
            raise ValueError('`tables` parameter must be "all" or of type list.')

        for table in table_names:
            if 'insert' in self.table_cmds[table]:
                print('Inserting into table: {}'.format(table))
                cmd = self.table_cmds[table]['insert']
                cur.execute(cmd) 
                self.conn.commit()

    def query_db(self, query):

        pass