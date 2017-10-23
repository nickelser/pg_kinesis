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

# Emits the following JSON to the specified Kinesis stream
# "lsn" is the WAL location of the operation
# "time" is the pg_kinesis host time when the replication message was received (the actual commit time is not available for most PG versions, but it should be very close); you can treat this as approximate (the LSN should be used to compare different records)
# "table" is the <schema>.<table> of the operation
# "operation" is the SQL operation that was performed
# "columns" is a mapping of the column name to a "new" value and optionally an "old" value (the current and the previous value, for an update or delete)
# each column value under "cur" or "old" has three fields:
#   "q" is if the value was quoted from postgres or not (false for simple values, like integers or booleans, null or (no-tuple-data))
#   "t" is the postgres type of the value
#   "v" is the string value
{"lsn":"1C/BB48DBF0","time":"2020-01-01T19:06:47-07:00","table":"public.customers","operation":"INSERT","columns":{"first_name":{"new":{"q":"true","t":"text","v":"Hello"}},"time":"2020-01-01T19:06:47-07:00","id":{"new":{"q":"false","t":"integer","v":"1"}},"last_name":{"new":{"q":"true","t":"text","v":"World"}}}}
{"lsn":"1C/BB48DCF8","time":"2020-01-01T19:06:47-07:00","table":"public.customers","operation":"INSERT","columns":{"first_name":{"new":{"q":"true","t":"text","v":"Goodbye"}},"id":{"new":{"q":"false","t":"integer","v":"2"}},"last_name":{"new":{"q":"true","t":"text","v":"World"}}}}
{"lsn":"1C/BB48DDB8","time":"2020-01-01T19:06:47-07:00","table":"public.customers","operation":"UPDATE","columns":{"first_name":{"new":{"q":"true","t":"text","v":"Hello"}},"time":"2020-01-01T19:06:47-07:00","id":{"new":{"q":"false","t":"integer","v":"1"}},"last_name":{"new":{"q":"true","t":"text","v":"Friends"}}}}
{"lsn":"1C/BB48DE78","time":"2020-01-01T19:06:47-07:00","table":"public.customers","operation":"DELETE","columns":{"id":{"old":{"q":"false","t":"integer","v":"2"}}}}

# With REPLICA IDENTITY FULL
# (notice the added "old" field)
{"lsn":"1C/BB48E068","time":"2020-01-01T19:06:47-07:00","table":"public.customers","operation":"UPDATE","columns":{"first_name":{"new":{"q":"true","t":"text","v":"Hello"}},"id":{"new":{"q":"false","t":"integer","v":"1"}},"last_name":{"new":{"q":"true","t":"text","v":"Replica Friends"},"old":{"q":"true","t":"text","v":"Friends"}}}}
```

## DDL changes

Note that logical replication only replicates changes to the actual table data, not changes to the table structure (DDL queries).

If you wish to transmit DDL changes as well, you can use the following snippet to record the DDL changes in a table, which you can point to a separate DDL stream (using the `-t pg_kinesis_ddl_audit` option of `pg_kinesis`) and then simply execute the contents of the `ddl_query` field. Execute the following on the source database (ensuring you are in the correct schema/db):

```sql
CREATE TABLE pg_kinesis_ddl_audit
(
  id           bigserial PRIMARY KEY,
  created_at   timestamp DEFAULT current_timestamp,
  db_user      varchar(64) DEFAULT current_user,
  tag          varchar(64),
  ddl_query    text
);

CREATE OR REPLACE FUNCTION pg_kinesis_record_ddl()
  RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
  AS $$
  DECLARE ddl_query_txt text;
BEGIN
  SELECT current_query() INTO ddl_query_txt;

  INSERT INTO pg_kinesis_ddl_audit
    (tag, ddl_query)
  VALUES
    (tg_tag, ddl_query_txt);
END;
$$;

CREATE EVENT TRIGGER pg_kinesis_record_ddl_trigger ON ddl_command_end
EXECUTE PROCEDURE pg_kinesis_record_ddl();
```

After that, DDL changes will be reflected as JSON as well:

```SQL
alter table foo add column fuzzy text;

# would emit the following to the specified Kinesis stream:
{"time":"2020-01-01T19:06:47-07:00","lsn":"1C/C9C16D48","table":"public.pg_kinesis_ddl_audit","operation":"INSERT","columns":{"created_at":{"new":{"q":"true","t":"timestamp without time zone","v":"2017-10-15 19:07:06.563259"}},"db_user":{"new":{"q":"true","t":"character varying","v":"nickelser"}},"ddl_query":{"new":{"q":"true","t":"text","v":"ALTER TABLE FOO ADD COLUMN BAZ TEXT;"}},"id":{"new":{"q":"false","t":"bigint","v":"1"}},"tag":{"new":{"q":"true","t":"character varying","v":"ALTER TABLE"}}}}
```

If you are applying the changes on some target DB, you would need to parse `columns.ddl_query.new.v` or just execute it wholesale (if you are applying changes to a fellow Postgres node).

To remove the trigger, function, and table:

```SQL
DROP EVENT TRIGGER pg_kinesis_record_ddl_trigger;
DROP FUNCTION pg_kinesis_record_ddl;
DROP TABLE pg_kinesis_ddl_audit;
```

Note that you cannot access table rewrite DML statements (e.g., `ALTER TABLE FOO ADD COLUMN BAZ TEXT DEFAULT 'BAZ';`) as the rewrite is done into a temporary table (like `public.pg_temp_210974`) that has no mapping to the altered table.

You must ignore inserts/updates to these tables by ignoring tables that look like `pg_temp_<numbers>`. See [this discussion](http://www.postgresql-archive.org/BUG-14785-Logical-replication-does-not-work-after-adding-a-column-Bug-td5979218.html) for more details.

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/nickelser/pg_kinesis/issues)
- Fix bugs and [submit pull requests](https://github.com/nickelser/pg_kinesis/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features
