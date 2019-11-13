import requests
import time
import datetime

class GeolocateIPs(object):
    """ Using a list of IPs, retrieves IP geolocation information 
    from the freegeoip.app API. We are limited to 15,000 calls per 
    hour, so if a 403 message is returned, by default we pause for
    5 minutes before trying again. If an output path is supplied,
    the methods will scan that file for previously geolocated IPs,
    so as not to repeat the geolocations, to save API calls. 
    """

    def __init__(self):

        self.ip_set = set()
        self.output_lines = []
        self.failed_ips = [] # IPs that failed, for whatever reason

    def _query_api(self, ip_addr):
        """ Given an ip_addr as a string, query the freegeoip API and return the response

        Parameters
        ----------
        ip_addr: str
            IP address to query

        Returns
        -------
        requests request
        """

        url_ip = "https://freegeoip.app/csv/{}".format(ip_addr)

        headers = {
            'accept': "application/csv",
            'content-type': "application/csv"
        }

        response = requests.request("GET", url_ip, headers=headers)

        return response

    def load_ip_list(self, ip_list_path):
        """  Load the IP list from a file. This function assumes the 
        file is a list of IPs, one IP per line. It returns a set
        of the IPs (i.e. removes duplicates). 

        Parameters
        ----------
        ip_list_path: str
            Path to IP list. List should be a text file with one IP address per row. 

        Returns
        -------
        set
            A set of the IP list (removes duplicates)
        """

        with open(ip_list_path, 'r') as ipf:
            ips = ipf.readlines()
            ips = [ip.strip() for ip in ips] # remove newlines

        self.ip_set = set(ips)
        print('Found {} unique IP addresses.'.format(len(self.ip_set)))
        return self.ip_set

    def geo_ips(self, ip_set, output_path, write_interval=1000, sleep_interval=300):
        """ Geolocate IP addresses using the freegeoip.app API. This function takes
        a list or set of the IP addresses you wish to geolocate and an output file path. 
        The function queries the freegeoip.app api, obtaining a CSV of the possible 
        IP address geolocation (note there can always be some error with these methods). 
        After a set interval of IP calls, it will append all the newly obtained IPs to
        the the output file (it will only append, never overwrite).

        Parameters
        ----------
        ip_set: set or list
          The list of IP addresses to geolocate. 
        output_path: str
          The path to the output file to write geolocations
        write_interval: int, default 1000
          When calling a large number of IP addresses, you will periodically want to save
          the geolocations to a file, in case the program crashes. The `write_interval` 
          specifies how often to save the CSVs to a file. For example if `write_interval = 100`, 
          then every 100 API calls, the geolocations are saved to a file.
        sleep_interval: int, default 300
          API calls to the freegeoip.app are limited to 15,000 per hour. If you happen to exceed 
          that rate limit (indicated by a `403` http message), this program will sleep for
          `sleep_interval` seconds before trying again. 

        Returns
        -------
        None. All output is written to the `output_path` file. 

        """

        self.output_lines = []
        # write to file every 5000 geolocated IPs
        prev_write_record = 0
        record_count = 0

        # first find unique IPs
        with open(output_path, 'r') as geod:
            gathered = [g.split(',')[0] for g in geod]
            gathered = set(gathered)
        print('Previously geolocated {} IP addresses.'.format(len(gathered)))

        # make sure `ip_set` is the right type:
        if not isinstance(ip_set, set):
            # try to force ip_set to be a set
            if isinstance(ip_set, list):
                ip_set = set(ip_set)
            else:
                raise TypeError('`ip_set` parameter must be of type set or list.')

        need_geos = ip_set.difference(gathered)

        time_needed = round(len(need_geos) / 15000, 1) # this is the hourly rate limit
        msg = (len(need_geos), time_needed)
        # actual time may differ depending on API fetch speed
        print('Fetching {} new geolocations. This will take at least {} hours.'.format(msg[0], msg[1]))

        ssl_errors = []
        for ip in need_geos:
            record_count += 1
            try: 
                response = self._query_api(ip)
                ssl_errors = []
                if response.status_code == 200:
                    newline = ','.join((ip, response.text))
                    self.output_lines.append(newline)
                elif response.status_code == 403:
                    # print a sleep message
                    print('{}: Reached hourly query limit. Sleeping for {} seconds.'.\
                        format(datetime.datetime.now(), sleep_interval))
                    time.sleep(sleep_interval)
                else:
                    print('{}: Return code: {} for IP {}. Adding IP to `failed_ips` list.'.\
                        format(datetime.datetime.now(), response.status_code, ip))
                    self.failed_ips.append(ip)
            except requests.exceptions.SSLError as e:
                print('{}: SSL Error at record {}, IP {}'.\
                    format(datetime.datetime.now(), record_count, ip)) 
                self.failed_ips.append(ip)
                if len(ssl_errors) > 10: # too many failures in a row, let's rest awhile
                    print('{}: 10 SSL Errors in a row. Sleeping for awhile.'.\
                        format(datetime.datetime.now()))
                    time.sleep(sleep_interval)
                    ssl_errors = []
            if record_count % 1000 == 0:
                print('{}: Completed {} IP pulls.'.format(datetime.datetime.now(), record_count))

            # at the write interval, write file
            if record_count % write_interval == 0:
                print('{}: Writing IP geolocations for IPs {}-{} to file.'.\
                    format(datetime.datetime.now(), prev_write_record, record_count))
                with open(output_path, 'a') as outfile:
                    outfile.writelines(self.output_lines)
                self.output_lines = [] # reset output lines
                prev_write_record = record_count

        # write at the end too
        print('{}: Writing IP geolocations for IPs {}-{} to file.'.\
            format(datetime.datetime.now(), prev_write_record, record_count))
        with open(output_path, 'a') as outfile:
            outfile.writelines(self.output_lines)

        if len(self.failed_ips) > 0:
            print('Failed to write {} IPs. See `failed_ips` attribute of object.'.\
                    format(len(self.failed_ips)))