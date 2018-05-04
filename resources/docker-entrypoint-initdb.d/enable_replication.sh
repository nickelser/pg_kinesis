#!/bin/sh

echo 'Tests - Enabling Replication'
echo 'host replication all all md5' >> /var/lib/postgresql/data/pg_hba.conf

cat << EOF >> /var/lib/postgresql/data/postgresql.conf
log_connections = on
log_disconnections = on
log_statement = 'all'
log_replication_commands = on
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
EOF

kill -HUP 1
