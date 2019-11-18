# this script reads the config file and sets up the redshift cluster with the desired 
# attributes. If the redshift cluster is already up and running, it will notify the user.
from redshift import redshift
import time
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

def create_cluster(rs):
    """ Given a redshift.redshift object, create a new cluster. This function will check for completion every 30 
    seconds and return once the cluster is available.

    Parameters
    ----------
    rs: redshift.redshift object
    
    Returns
    -------
    None
    """

    rs.create_cluster(verbose=False)
    print('Creating cluster. Will check every 30 seconds for completed creation.')
    cluster_built = False
    while not cluster_built:
        print('Sleeping 30 seconds.')
        time.sleep(30)
        cluster_built = check_available(rs)

def main():
    """ This function checks if the redshift cluster, as specified in the config file, is available.
    If it is already available, it will print a message saying so. If not, it will attempt to create
    the cluster.
    """
    rs = redshift(config_file=CONFIG_FILENAME)
    
    # check if cluster already available
    try:
        clust_avail = check_available(rs)
    except rs_client.exceptions.ClusterNotFoundFault:
        clust_avail = False

    # if cluster not available, create it
    if not clust_avail:
        create_cluster(rs)        
    
    print(f'Cluster is available. Cluster information: \n{rs.get_cluster_info()}')    

if __name__ == "__main__":
    main()