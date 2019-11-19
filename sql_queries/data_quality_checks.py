from . import honeypot_sql

all_tables = [honeypot_sql.table_commands[tbl]['name'] for tbl in honeypot_sql.table_commands]

count_rows = 'SELECT COUNT(*) FROM {};'

reputation_joined = 'SELECT COUNT(*) FROM attacks WHERE attacker_risk IS NOT NULL;'

total_rows_fact = 'SELECT COUNT(*) FROM attacks;'