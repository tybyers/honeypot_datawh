
#  DROP TABLES
staging_honeypot_drop = "DROP TABLE IF EXISTS staging_honeypot;"
staging_ipgeo_drop = "DROP TABLE IF EXISTS staging_ipgeo;"
staging_reputation_drop = "DROP TABLE IF EXISTS staging_reputation;"

# CREATE TABLES
staging_reputation_create = ("""
CREATE TABLE IF NOT EXISTS staging_reputation
(IP           VARCHAR(255),
 Reliability  INTEGER,
 Risk         INTEGER,
 Type         VARCHAR(255),
 Country      VARCHAR(2),
 Locale       VARCHAR(255),
 Latitude     NUMERIC,
 Longitude    NUMERIC
);""")

staging_honeypot_create = ("""
CREATE TABLE IF NOT EXISTS staging_honeypot
(id VARCHAR)
;""")

staging_ipgeo_create = ("""
CREATE TABLE IF NOT EXISTS staging_ipgeo
(id VARCHAR)
;""")


table_commands = {
    'staging_honeypot': {
        'drop': staging_honeypot_drop,
        'create': staging_honeypot_create,
        'insert': ''
    },
    'staging_ipgeo': {
        'drop': staging_ipgeo_drop,
        'create': staging_ipgeo_create,
        'insert': ''
    },
    'staging_reputation': {
        'drop': staging_reputation_drop,
        'create': staging_reputation_create,
        'insert': ''
    },
}

# 'honeypot_staging': {
#         'delete': '',
#         'create': '',
#         'insert': ''
#     },