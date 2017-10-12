# pg_kinesis

Replicate some or all of Postgres commits using logical replication to AWS Kinesis.

## Install

pg_kinesis is a command line tool. To install, run:

```sh
go install github.com/nickelser/pg_kinesis
```

This will give you the `pg_kinesis` command, assuming your Go paths are setup correctly.

## Usage

```sh
Usage:
  pg_kinesis --source=postgres://user:pass@src.example.com/sourcedb --stream=example_stream --tables=public.users,public.events

Options:
  -h --help             Show this message.
  --slot                Logical replication slot on the source DB. (default: pg_kinesis)
  --source              Source Postgres database URI/DSN. You can also use the libpq-standard PGDATABASE/PGHOST/&c. environment variables (which are tried first; --source overrides the environment).
  --stream              Name of the AWS Kinesis stream to stream to. (required)
  --create              Create the specified replication slot before starting replication.
  --drop                Drop the specified replication slot. Exits upon success.
  -t --table            Table to transfer. Multiple tables can be selected by writing multiple -t switches. Defaults to all tables. The matching semantics are the same as psql (https://www.postgresql.org/docs/current/static/app-psql.html#app-psql-patterns)
  -T --exclude-table    Table to exclude. Defaults to excluding no tables. The matching logic is the same as for -t; -T has higher precedence than -t.
  --retry-initial       If this flag is present, retry the initial connection to the replication slot; useful for high-availability setups where the same pg_kinesis command is run from multiple hosts.
```

Sample psql input and Kinesis stream output:

```SQL
CREATE TABLE customers (id serial primary key, first_name text, last_name text);

INSERT INTO customers (first_name, last_name) VALUES ('Hello', 'World');
INSERT INTO customers (first_name, last_name) VALUES ('Goodbye', 'World');
UPDATE customers SET last_name = 'Friends' where first_name = 'Hello';
DELETE FROM customers WHERE first_name = 'Goodbye';

# set the REPLICA IDENTITY to FULL to get the old as well as updated values for updaes
ALTER TABLE customers REPLICA IDENTITY FULL;

UPDATE customers SET last_name = 'Replica Friends' where first_name = 'Hello';

# Emits JSON
# "lsn" is the WAL location of the operation
# "table" is the <schema>.<table> of the operation
# "operation" is the SQL operation that was performed
# "columns" is a mapping of the column name to "cur" and optionally "old" (the current and the previous value, respectively)
# each column value under "cur" or "old" has three fields:
#   "q" is if the value was quoted or not (false for null or (no-tuple-data))
#   "t" is the postgres type of the value
#   "v" is the string value
{"lsn":"1C/B01D63D8","table":"public.customers","operation":"INSERT","columns":{"first_name":{"cur":{"q":"true","t":"text","v":"Hello"}},"id":{"cur":{"q":"false","t":"integer","v":"1"}},"last_name":{"cur":{"q":"true","t":"text","v":"World"}}}}
{"lsn":"1C/B01D64E0","table":"public.customers","operation":"INSERT","columns":{"first_name":{"cur":{"q":"true","t":"text","v":"Goodbye"}},"id":{"cur":{"q":"false","t":"integer","v":"2"}},"last_name":{"cur":{"q":"true","t":"text","v":"World"}}}}
{"lsn":"1C/B01D65D8","table":"public.customers","operation":"UPDATE","columns":{"first_name":{"cur":{"q":"true","t":"text","v":"Hello"}},"id":{"cur":{"q":"false","t":"integer","v":"1"}},"last_name":{"cur":{"q":"true","t":"text","v":"Friends"}}}}
{"lsn":"1C/B01D6698","table":"public.customers","operation":"DELETE","columns":{"id":{"old":{"q":"false","t":"integer","v":"2"}}}}

# With REPLICA IDENTITY FULL
# (notice the added "old" field)
{"lsn":"1C/B01DF8F8","table":"public.customers","operation":"UPDATE","columns":{"first_name":{"cur":{"q":"true","t":"text","v":"Hello"}},"id":{"cur":{"q":"false","t":"integer","v":"1"}},"last_name":{"cur":{"q":"true","t":"text","v":"Replica Friends"},"old":{"q":"true","t":"text","v":"Friends"}}}}
```

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/nickelser/pg_kinesis/issues)
- Fix bugs and [submit pull requests](https://github.com/nickelser/pg_kinesis/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features
