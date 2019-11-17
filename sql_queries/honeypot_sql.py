
#  DROP TABLES
staging_honeypot_drop = "DROP TABLE IF EXISTS staging_honeypot;"
staging_ipgeo_drop = "DROP TABLE IF EXISTS staging_ipgeo;"
staging_reputation_drop = "DROP TABLE IF EXISTS staging_reputation;"
dim_glastopf_drop = "DROP TABLE IF EXISTS glastopf_events;"
dim_amun_drop = "DROP TABLE IF EXISTS amun_events;"
dim_dionaea_drop = "DROP TABLE IF EXISTS dionaea_events;"
dim_snort_drop = "DROP TABLE IF EXISTS snort_events;"
dim_ipgeo_drop = "DROP TABLE IF EXISTS ipgeo;"
dim_reputation_drop = "DROP TABLE IF EXISTS reputation;"
fact_attacks_drop = "DROP TABLE IF EXISTS attacks;"

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
    Latitude     FLOAT4,
    Longitude    FLOAT4
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
    latitude      FLOAT4,
    longitude     FLOAT4,
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

dim_glastopf_create = ("""
CREATE TABLE IF NOT EXISTS glastopf_events
(
    id                  VARCHAR(255) PRIMARY KEY,
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
    victimIP            VARCHAR(255)
);""")

dim_amun_create = ("""
CREATE TABLE IF NOT EXISTS amun_events
(
    id                  VARCHAR(255) PRIMARY KEY,
    ident               VARCHAR(255),
    normalized          BOOLEAN,
    timestamp           TIMESTAMP,
    channel             VARCHAR(255),
    attackerIP          VARCHAR(255),
    attackerPort        NUMERIC,
    victimIP            VARCHAR(255),
    victimPort          NUMERIC,
    connectionType      VARCHAR(255)
);""")

dim_dionaea_create = ("""
CREATE TABLE IF NOT EXISTS dionaea_events
(
    id                  VARCHAR(255) PRIMARY KEY,
    ident               VARCHAR(255),
    normalized          BOOLEAN,
    timestamp           TIMESTAMP,
    channel             VARCHAR(255),
    attackerIP          VARCHAR(255),
    attackerPort        NUMERIC,
    victimPort          NUMERIC,
    victimIP            VARCHAR(255),
    connectionType      VARCHAR(255),
    connectionProtocol  VARCHAR(255),
    connectionTransport VARCHAR(10),
    remoteHostname      VARCHAR(255)
);""")

dim_snort_create = ("""
CREATE TABLE IF NOT EXISTS snort_events
(
    id                  VARCHAR(255) PRIMARY KEY,
    ident               VARCHAR(255),
    normalized          BOOLEAN,
    timestamp           TIMESTAMP,
    channel             VARCHAR(255),
    attackerIP          VARCHAR(255),
    victimIP            VARCHAR(255),
    connectionType      VARCHAR(255),
    connectionProtocol  VARCHAR(255),
    priority            INTEGER,
    header              VARCHAR(255),
    signature           VARCHAR(255),
    sensor              VARCHAR(255)
);""")

dim_ipgeo_create = ("""
CREATE TABLE IF NOT EXISTS ipgeo
(
    id INT PRIMARY KEY
);""")

dim_reputation_create = ("""
CREATE TABLE IF NOT EXISTS reputation
(
    id INT  PRIMARY KEY
)
;""")

fact_attacks_create = ("""
CREATE TABLE IF NOT EXISTS attacks
(
    id INT PRIMARY KEY
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

# INSERT INTO TABLES
dim_glastopf_insert = ("""
INSERT INTO glastopf_events (
    id, ident, normalized, timestamp, channel, pattern, filename,
    request_raw, request_url, attackerIP, attackerPort, victimPort, victimIP)
  (SELECT DISTINCT id, ident, normalized, "timestamp", channel, pattern, filename,
                   request_raw, request_url, attackerIP, attackerPort, victimPort, victimIP
    FROM staging_honeypot
    WHERE channel = 'glastopf.events')
;""")

dim_amun_insert = ("""
INSERT INTO amun_events (
    id, ident, normalized, timestamp, channel, attackerIP, attackerPort, 
    victimIP, victimPort, connectionType)
  (SELECT DISTINCT id, ident, normalized, "timestamp", channel, attackerIP, attackerPort, 
    victimIP, victimPort, connectionType
    FROM staging_honeypot
    WHERE channel = 'amun.events')
;""")

dim_dionaea_insert = ("""
INSERT INTO dionaea_events (
    id, ident, normalized, timestamp, channel, attackerIP, attackerPort, victimPort, 
    victimIP, connectionType, connectionProtocol, connectionTransport, remoteHostname)
  (SELECT DISTINCT id, ident, normalized, "timestamp", channel, attackerIP, attackerPort, victimPort, 
    victimIP, connectionType, connectionProtocol, connectionTransport, remoteHostname
    FROM staging_honeypot
    WHERE channel = 'dionaea.connections')
;""")

dim_snort_insert = ("""
INSERT INTO snort_events (
    id, ident, normalized, timestamp, channel, attackerIP, victimIP, connectionType,
    connectionProtocol, priority, header, signature, sensor)
  (SELECT DISTINCT id, ident, normalized, "timestamp", channel, attackerIP, victimIP, connectionType,
    connectionProtocol, priority, header, signature, sensor
    FROM staging_honeypot
    WHERE channel = 'snort.alerts')
;""")

# the following dict allows us to more easily control which drop/create/copy/insert functions we want to call
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
    'dim_glastopf': {
        'drop': dim_glastopf_drop,
        'create': dim_glastopf_create,
        'insert': dim_glastopf_insert
    },
    'dim_amun': {
        'drop': dim_amun_drop,
        'create': dim_amun_create,
        'insert': dim_amun_insert
    },
    'dim_dionaea': {
        'drop': dim_dionaea_drop,
        'create': dim_dionaea_create,
        'insert': dim_dionaea_insert
    },
    'dim_snort': {
        'drop': dim_snort_drop,
        'create': dim_snort_create,
        'insert': dim_snort_insert
    },
    'dim_ipgeo': {
        'drop': dim_ipgeo_drop,
        'create': dim_ipgeo_create
    },
    'dim_reputation': {
        'drop': dim_reputation_drop,
        'create': dim_reputation_create
    },
    'fact_attacks': {
        'drop': fact_attacks_drop,
        'create': fact_attacks_create
    }
}