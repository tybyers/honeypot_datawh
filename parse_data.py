# This modules in this file parse data from various formats and return pd Data Frames, which should 
# be then saved to CSV and uploaded to S3. 

import json
import pandas as pd


def honeypot_json_to_df(filename):
    """ Takes a json file of honeypot log data, which contains at least 4 different "channels" (honeypot types)
    and returns a "standardized" data frame. All four honeypot types log their data slightly differently, so this 
    process takes the most important fields from the json entries and does its best to create a standardized Data
    Frame. 

    Parameters
    ----------
    filename: str
      Filename for the raw json file to parse into a Data Frame.

    Returns
    -------
    pd DataFrame
      A "standardized" Data Frame of the important honeypot log fields. 

    """

    with open(filename) as f:
        jsons = [json.loads(l) for l in f.readlines()]

    def build_df_row(j):
        try:
            if j['channel'] == 'glastopf.events':
                payload = json.loads(j['payload'])
                row = pd.DataFrame({
                    'id'             : j['_id']['$oid'],
                    'ident'          : j['ident'],
                    'normalized'     : j['normalized'],
                    'timestamp'      : j['timestamp']['$date'],
                    'channel'        : j['channel'],
                    'pattern'        : payload['pattern'],
                    'filename'       : payload['filename'],
                    'request_raw'    : payload['request_raw'],
                    'request_url'    : payload['request_url'],
                    'attackerIP'     : payload['source'][0],
                    'attackerPort'   : payload['source'][1],
                    'victimPort'     : 80, # from documentation,
                    'victimIP'       : 0 # from documentation
                }, index = [0])
            elif j['channel'] == 'amun.events':
                payload = json.loads(j['payload'])
                row = pd.DataFrame({
                    'id'             : j['_id']['$oid'],
                    'ident'          : j['ident'],
                    'normalized'     : j['normalized'],
                    'timestamp'      : j['timestamp']['$date'],
                    'channel'        : j['channel'],
                    'attackerIP'     : payload['attackerIP'],
                    'attackerPort'   : payload['attackerPort'],
                    'victimIP'       : payload['victimIP'],
                    'victimPort'     : payload['victimPort'],
                    'connectionType' : payload['connectionType']      
                }, index = [0])
            elif j['channel'] == 'dionaea.connections':
                payload = json.loads(j['payload'])
                row = pd.DataFrame({
                    'id'                  : j['_id']['$oid'],
                    'ident'               : j['ident'],
                    'normalized'          : j['normalized'],
                    'timestamp'           : j['timestamp']['$date'],
                    'channel'             : j['channel'],
                    'attackerIP'          : payload['remote_host'],
                    'attackerPort'        : payload['remote_port'],
                    'victimIP'            : payload['local_host'],
                    'victimPort'          : payload['local_port'],
                    'connectionType'      : payload['connection_type'],
                    'connectionTransport' : payload['connection_transport'],
                    'connectionProtocol'  : payload['connection_protocol'],
                    'remoteHostname'      : payload['remote_hostname']
                }, index = [0])
            elif j['channel'] == 'snort.alerts':
                payload = json.loads(j['payload'])
                row = pd.DataFrame({
                    'id'                  : j['_id']['$oid'],
                    'ident'               : j['ident'],
                    'normalized'          : j['normalized'],
                    'timestamp'           : j['timestamp']['$date'],
                    'channel'             : j['channel'],
                    'attackerIP'          : payload['source_ip'],
                    'victimIP'            : payload['destination_ip'],
                    'connectionType'      : payload['classification'],
                    'connectionProtocol'  : payload['proto'],
                    'priority'            : payload['priority'],
                    'header'              : payload['header'],
                    'signature'           : payload['signature'],
                    'sensor'              : payload['sensor']
                }, index = [0])
            else:
                row = pd.DataFrame()
            return row
        except:
            print(sys.exc_info())

    df = pd.concat([build_df_row(j) for j in jsons], sort=False)
    df.reset_index(inplace=True, drop=True)

    return df

def reputation_raw_to_df(filename):
    """ Takes a raw AlienVault Reputation Data file and conducts a few mild parsing steps to allow it 
    to be saved to CSV.

    Parameters
    ----------
    filename: str
      Filename for the raw #-delimted file to parse into a Data Frame.

    Returns
    -------
    pd Data Frame
      A Data Frame with some modifications from the original for easier use downstream. 

    """

    colnames = ['IP', 'Reliability', 'Risk', 'Type', 'Country', 'Locale', 'Coords', 'x']
    rep = pd.read_csv(filename, sep='#', header=None, names=colnames)
    def get_lat_long(coords):
        # split "coords" column into lat/long
        coords = coords.split(',')
        return coords[0], coords[1]
    rep["Latitude"], rep["Longitude"] = rep["Coords"].str.split(',', 1).str
    rep.drop(['Coords', 'x'], inplace=True)

    return rep
