# this script builds the tables for the cluster. The cluster must have been
# previously built using the `cluster_setup.py` script or other means.
from honeypot_redshift import honeypot_redshift
CONFIG_FILENAME = './aws.cfg'  # note: not included in GH repo for privacy. See `aws_example.cfg` for example


def main():
    """ This function checks the redshift connection. If the database is available and endpoint information
    is up, then it will delete old tables and create new ones. If not...it won't. 

    """
    hrs = honeypot_redshift(config_file=CONFIG_FILENAME)
    # make sure the endpoint information is attached
    info = hrs.get_cluster_info()

    # if endpoint isn't attached, nothing we can do, except notify the user
    can_build_tables = False
    if info['ClusterStatus'] == 'available' and 'Endpoint' in info:
        if isinstance(info['Endpoint'], dict) and 'Address' in info['Endpoint']:
            print('Connecting to Database.')
            hrs.db_connect()
            print('Deleting all tables.')
            hrs.delete_tables(tables='all')
            print('Creating new tables.')
            hrs.create_tables(tables='all')
            can_build_tables = True

    if not can_build_tables:
        print('Cannot connect to database at this time. Please check redshift database status: \n{}'.\
            format(info))

if __name__ == '__main__':
    main()