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

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/nickelser/pg_kinesis/issues)
- Fix bugs and [submit pull requests](https://github.com/nickelser/pg_kinesis/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features
