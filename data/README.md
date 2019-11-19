# Data

This folder holds raw and processed data prior to uploading to S3 for transfer to the honeypot data warehouse. For the data warehouse diagram and data dictionary, please see the [Data Dictionary](./Data_Dictionary.md) document in this directory.

## Honeypot Data

The honeypot data is the core of this project. In this directory, we have the following data files:

  * honeypot_sample.json - 50-line sample of the raw honeypot data. 
  * honeypot_10ksamp.csv - 10,000-line sample of the processed honeypot data, in CSV format.  

For the full, nearly 1-million line honeypot JSON data, please download and unzip the original data set at: https://www.secrepo.com/honeypot/honeypot.json.zip

## IP Data

The following data sets are used for the IP geolocations:

  * honeypot_all_attackerips.csv - a 186k-line data set, one IP per line, of all the unique IP addresses in the honeypot data. 
  * ip_geos.csv - The IP geolocations for the above IP addresses, obtained from freegeoip.app. 

## Reputation Data

We obtained the AlienVault reputation data from https://reputation.alienvault.com/reputation.data at 2019-11-10, 11:01 AM PST. 

  * reputation_20191110_110100.dat - Original, #-delimited file of reputation data. 
  * reputation.csv - Essentially the same data set as above, but comma-delimited.  

## Schema Data

  * honeypot_db_schema.xml - Schema download -- built schema using the free tool on draw.io.  
  * honeypot_db_schema.png - Schema as PNG.  