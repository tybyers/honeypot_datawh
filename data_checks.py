# this script runs unit tests on the honeypot redshift tables after they have been created and populated
from honeypot_redshift import honeypot_redshift
from sql_queries import data_quality_checks
CONFIG_FILENAME = './aws.cfg'  # note: not included in GH repo for privacy. See `aws_example.cfg` for example

def test_tables_populated(hrs):
    """ Tests the tables are populated with any data at all.

    Parameters
    ----------
    hrs: honeypot_redshift object

    Returns
    -------
    None
    """

    print('Testing Tables are Populated')
    cur = hrs.conn.cursor()

    all_tables = data_quality_checks.all_tables
    for tbl in all_tables:
        query = data_quality_checks.count_rows.format(tbl)
        cur.execute(query)
        assert cur.fetchone()[0] > 0

def test_reputation_fact(hrs):
    """ Tests reputation data made it into the fact table. 

    Parameters
    ----------
    hrs: honeypot_redshift object

    Returns
    -------
    None
    """
    print('Testing reputation information successfully joined to fact table. ')
    cur = hrs.conn.cursor()

    query = data_quality_checks.reputation_joined
    cur.execute(query)
    assert cur.fetchone()[0] > 0

def test_more_total_fact_reputation(hrs):
    """ Tests reputation data made it into the fact table BUT that there are fewer rows there 
    than in the entire fact table. 

    Parameters
    ----------
    hrs: honeypot_redshift object

    Returns
    -------
    None
    """
    print('Testing not all the rows will have reputation information.')
    cur = hrs.conn.cursor()

    query = data_quality_checks.reputation_joined
    cur.execute(query)
    rep_rows = cur.fetchone()[0]

    query2 = data_quality_checks.total_rows_fact
    cur.execute(query2)
    fact_rows = cur.fetchone()[0]

    assert rep_rows < fact_rows

def main():

    hrs = honeypot_redshift(config_file=CONFIG_FILENAME)
    # make sure the endpoint information is attached
    info = hrs.get_cluster_info()

    # connect to database
    hrs.db_connect()

    # run tests
    test_tables_populated(hrs)
    test_reputation_fact(hrs)
    test_more_total_fact_reputation(hrs)

if __name__ == '__main__':
    main()