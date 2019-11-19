# this script populates the tables once they have been built
from honeypot_redshift import honeypot_redshift
import data_checks
CONFIG_FILENAME = './aws.cfg'  # note: not included in GH repo for privacy. See `aws_example.cfg` for example

def populate_tables(hrs):
    """ This function populates the redshift tables, first by copying into the staging tables, then
    inserting into the dim and fact tables.

    Parameters
    ----------
    hrs: honeypot_redshift object

    Returns
    -------
    None
    """

    print('Copying into staging tables.')
    hrs.copy_into_tables(tables='all')
    print('Inserting into dim and fact tables.')
    hrs.insert_into_tables(tables='all')

def data_quality_checks(hrs):
    """ Run the data quality checks.

    Parameters
    ----------
    hrs: honeypot_redshift object

    Returns
    -------
    None
    """
    
    data_checks.test_tables_populated(hrs)
    data_checks.test_reputation_fact(hrs)
    data_checks.test_more_total_fact_reputation(hrs) 


def main():
    """ This function checks the redshift connection. If the database is available and endpoint information
    is up, then it will connect to the database and call the `populate_tables` function.
    """

    hrs = honeypot_redshift(config_file=CONFIG_FILENAME)
    # make sure the endpoint information is attached
    info = hrs.get_cluster_info()

    # if endpoint isn't attached, nothing we can do, except notify the user
    can_connect = False
    if info['ClusterStatus'] == 'available' and 'Endpoint' in info:
        if isinstance(info['Endpoint'], dict) and 'Address' in info['Endpoint']:
            print('Connecting to Database.')
            hrs.db_connect()
            populate_tables(hrs)
            data_quality_checks(hrs)
            can_connect = True

    if not can_connect:
        print('Cannot connect to database at this time. Please check redshift database status: \n{}'.\
            format(info))


if __name__ == '__main__':
    main()