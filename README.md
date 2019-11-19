# About

This repo holds my work for the Capstone project for the Udacity [Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027). This README contains the TL;DR information. 

For more information, please see the full writeup: TODO: LINK HERE. 

# Project Summary

TODO: SUMMARY PARAGRAPH FROM FINAL REPORT. 

# Data Sources

We retrieved our original data from a variety of sources. **The total "lines of data" for this project is 1,254,620, from a mixture of JSON, API, and #-separated values.**    

* Honeypot data: https://www.secrepo.com/honeypot/honeypot.json.zip   
    * Format (after unzipping): JSON  
    * Record count: 994,692  
    * This data was mostly collected in 2014 and 2015. Mike Sconzo [@sooshie](https://github.com/sooshie) made the data available via his www.secrepo.com site, and also provided an [iPython notebook](https://www.secrepo.com/honeypot/BSidesDFW%20-%202014.ipynb) with information about how to parse the data from JSON.   
* Free Geo IP app: https://freegeoip.app/ 
    * Format: API/CSV  
    * Record count: 185,936  
    * How obtained: I created a list of unique "attacker IP" addresses from the Honeypot data, and then used functions I wrote in the `geolocate_ips.py` file to query the API and save off the results.  
* AlienVault Reputation Database: https://reputation.alienvault.com/reputation.data
    * Format: #-delimited file
    * Record count: 74,892  
    * This file is updated hourly. I downloaded 2019-11-10 at approximately 11:00 am.

Before loading the data to S3, I preprocessed all of the data in this file using the `parse_data.py` scripts, converting all of the data to Pandas Data Frames. Then I saved those files as CSVs and uploaded to S3. From there, I used the scripts and modules in `create_tables`, `etl`, and `honeypot_redshift.py` to build the Data Warehouse.  

# Repo Contents

This section contains more detailed information about what you may find in this repository.  

## Scripts

The following scripts help facilitate the cluster creation and database population. `run <script>` from iPython terminal, in this order:  

  * `cluster_delete.py` - Delete the cluster if you wish to run from scratch.  
  * `cluster_setup.py` -- Set up the cluster and populate IAM role and Endpoints.  
  * `create_tables.py` -- Build the data warehouse tables, dropping all existing tables.  
  * `etl.py` -- Populate the data warehouse tables.  
  * `data_checks.py` -- Data quality/insertion checks.  

## Classes/Modules  

The following classes and modules can be run from Jupyter notebooks or called by other processes. The `redshift.py` and `honeypot_redshift.py` files are called by the scripts from the previous section. 

  * `parse_data.py` -- Take the raw data and convert to Pandas Data Frames, which we then saved to CSV (from a Jupyter notebook).  
  * `geolocate_ips.py` -- Takes a list of IPs and queries the freegeoip.app API to return IP geolocations. 
  * `redshift.py` -- Class to connect to, and delete, a redshift cluster, based on a configuration file. Note: much of the capability and ideas for this were taken from the Unit 2 project for the Data Engineering nanodegree (though it wasn't packaged this nicely).  
  * `honeypot_redshift.py` -- Extends the `redshift` class to provide specific table creation, deletion, copying, and insertion capabilities for the data warehouse.  


## Other Files 

  * `final_report.ipynb` -- Jupyter notebook with the final report for this project.  
  * `aws_example.cfg` -- An example configuration file. Ours was git ignored to keep KEY/SECRETs out of GitHub.  
  * `working` directory -- Several ipynbs acting as our "scratch pads" throughout the development process. Some of them may not run due to module name changes over time. 

## Data

The [data](./data) directory contains a README and Data Dictionary with more information about the data.  

## SQL Queries

SQL queries are in the [sql_queries](./sql_queries) directory. This directory contains:

  * `honeypot_sql.py` -- Queries for dropping, creating, copying into, and inserting into the data tables.  
  * `data_quality_checks.py` -- Queries for the data quality checks.  
