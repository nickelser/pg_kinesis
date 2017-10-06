# pg_kinesis TODO

Replicate some or all of a Postgres database using logical replication and the built-in test_decoder.

## Install

pg_replicatelogical is a command line tool. To install, run:

```sh
go install github.com/nickelser/pg_replicatelogical
```

This will give you the `pg_replicatelogical` command, assuming your Go paths are setup correctly.

## Usage

```sh
pg_replicatelogical --source=postgres://user:pass@src.example.com/sourcedb --target=postgres://user:pass@tgt.example.com/targetdb --create --slot=users_events_slot_1 # run --create only once, per slot you need!
pg_replicatelogical --source=postgres://user:pass@src.example.com/sourcedb --target=postgres://user:pass@tgt.example.com/targetdb --tables=public.users,public.events --slot=users_events_slot_1
```

TODO: more examples! use `pg_replicatelogical --help` for the full help text.

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/nickelser/pg_replicatelogical/issues)
- Fix bugs and [submit pull requests](https://github.com/nickelser/pg_replicatelogical/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features
