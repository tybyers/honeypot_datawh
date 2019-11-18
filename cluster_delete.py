# this script deletes the redshift cluster if it is available
from redshift import redshift
import boto3
CONFIG_FILENAME = './aws.cfg'  # note: not included in GH repo for privacy. See `aws_example.cfg` for example
rs_client = boto3.client('redshift', region_name='us-west-2')

def check_available(rs):
    """ Given a redshift object, determine if the cluster is available. 

    Parameters
    ----------
    rs: redshift.redshift object
    
    Returns
    -------
    bool; `True` if cluster is available, `False` if being created. Throws an error if other status found.

    """
    info = rs.get_cluster_info()
    if info['ClusterStatus'] == 'available':
        return True
    elif info['ClusterStatus'] == 'deleting':
        raise AttributeError(f'Cluster is currently deleting. Please wait and try again.\n{info}')
    elif info['ClusterStatus'] == 'creating': 
        return False
    
    raise NameError(f'Could not get cluster status information. Current information about the cluster: \n{info}')

def main():
    """ Attempts to delete the redshift cluster. If it cannot, it will notify the user and ask
    them to try again. 
    """
    rs = redshift(config_file=CONFIG_FILENAME)
    
    # check if cluster already available
    try:
        clust_avail = check_available(rs)
    except rs_client.exceptions.ClusterNotFoundFault:
        clust_avail = False


    if clust_avail:
        rs.shutdown_cluster()
        print(f'Shutting down cluster. Cluster info: \n{rs.get_cluster_info()}')
    else:
        print(f'Cannot shut down cluster in current state. Please try again.')

if __name__ == '__main__':
    main()