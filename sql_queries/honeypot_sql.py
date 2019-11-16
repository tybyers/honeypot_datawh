
#  DROP TABLES
staging_honeypot_drop = "DROP TABLE IF EXISTS staging_honeypot;"
staging_ipgeo_drop = "DROP TABLE IF EXISTS staging_ipgeo;"
staging_reputation_drop = "DROP TABLE IF EXISTS staging_reputation;"

# CREATE TABLES
staging_reputation_create = ("""
CREATE TABLE IF NOT EXISTS staging_reputation
(
    IP           VARCHAR(255),
    Reliability  INTEGER,
    Risk         INTEGER,
    Type         VARCHAR(255),
    Country      VARCHAR(2),
    Locale       VARCHAR(255),
    Latitude     NUMERIC,
    Longitude    NUMERIC
);""")

staging_ipgeo_create = ("""
CREATE TABLE IF NOT EXISTS staging_ipgeo
(
    IP_orig       VARCHAR(255),
    IP            VARCHAR(255),
    country_code  VARCHAR(2),
    country_name  VARCHAR(255),
    region_code   VARCHAR(3),
    region_name   VARCHAR(255),
    city          VARCHAR(255),
    zip_code      VARCHAR(10),
    time_zone     VARCHAR(255),
    latitude      NUMERIC,
    longitude     NUMERIC,
    metro_code    INT
);""")

staging_honeypot_create = ("""
CREATE TABLE IF NOT EXISTS staging_honeypot
(
    id                  VARCHAR(255),
    ident               VARCHAR(255),
    normalized          BOOLEAN,
    timestamp           TIMESTAMP,
    channel             VARCHAR(255),
    pattern             VARCHAR(255),
    filename            VARCHAR(255),
    request_raw         VARCHAR(MAX),
    request_url         VARCHAR(MAX),
    attackerIP          VARCHAR(255),
    attackerPort        NUMERIC,
    victimPort          NUMERIC,
    victimIP            VARCHAR(255),
    connectionType      VARCHAR(255),
    connectionProtocol  VARCHAR(255),
    priority            INTEGER,
    header              VARCHAR(255),
    signature           VARCHAR(255),
    sensor              VARCHAR(255),
    connectionTransport VARCHAR(10),
    remoteHostname      VARCHAR(255)
);""")

# COPY INTO TABLES
staging_reputation_copy = ("""
COPY staging_reputation 
FROM {}
IGNOREHEADER 1
credentials 'aws_iam_role={}'
region 'us-west-2' compupdate off
CSV;
""") #.format(REPUTATION_DATA, IAM_ROLE)

staging_ipgeo_copy = ("""
COPY staging_ipgeo 
FROM {}
IGNOREHEADER 1
credentials 'aws_iam_role={}'
region 'us-west-2' compupdate off
CSV;
""") #.format(IPGEO_DATA, IAM_ROLE)

staging_honeypot_copy = ("""
COPY staging_honeypot 
FROM {}
IGNOREHEADER 1
credentials 'aws_iam_role={}'
region 'us-west-2' compupdate off
TIMEFORMAT 'auto'
CSV;
""") #.format(HONEYPOT_DATA, IAM_ROLE)


table_commands = {
    'staging_honeypot': {
        'drop': staging_honeypot_drop,
        'create': staging_honeypot_create,
        'copy': staging_honeypot_copy
    },
    'staging_ipgeo': {
        'drop': staging_ipgeo_drop,
        'create': staging_ipgeo_create,
        'copy': staging_ipgeo_copy
    },
    'staging_reputation': {
        'drop': staging_reputation_drop,
        'create': staging_reputation_create,
        'copy': staging_reputation_copy
    },
}

# 'honeypot_staging': {
#         'delete': '',
#         'create': '',
#         'insert': ''
#     },