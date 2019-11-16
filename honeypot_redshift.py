import pandas as pd
import boto3
from redshift import redshift
import configparser
import psycopg2
from sql_queries import honeypot_sql as SQL_QUERIES

class honeypot_redshift(redshift):

    def __init__(self, config_file='aws.cfg'):
        super().__init__(config_file)

        config = configparser.ConfigParser()
        with open(config_file) as cf:
            config.read_file(cf)
        self.IAM_ROLE = config.get('IAM_ROLE', 'ARN')
        self.s3_honeypot = config.get('S3', 'HONEYPOT_DATA')
        self.s3_reputation = config.get('S3', 'REPUTATION_DATA')
        self.s3_ipgeo = config.get('S3', 'IP_GEO_DATA')
        self.table_cmds = SQL_QUERIES.table_commands
        self.conn = None # db connection

    def db_connect(self):

        self.get_cluster_info()
        self.conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".\
            format(self.DWH_ENDPOINT, self.DWH_DB, self.DWH_DB_USER, self.DWH_DB_PASSWORD, self.DWH_PORT))
    

    def delete_tables(self, tables='all'):

        # at some point, make the "tables" parameter work
        cur = self.conn.cursor()
        for table in self.table_cmds:
            cur.execute(self.table_cmds[table]['drop'])

    def create_tables(self, tables='all'):

        # at some point, make the "tables" parameter work
        cur = self.conn.cursor()
        for table in self.table_cmds:
            cur.execute(self.table_cmds[table]['create']) 

    def insert_into_tables(self, tables='all'):

        pass