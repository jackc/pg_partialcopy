[![Build Status](https://github.com/jackc/pg_partialcopy/actions/workflows/ci.yml/badge.svg)](https://github.com/jackc/pg_partialcopy/actions/workflows/ci.yml)

# pg_partialcopy

pg_partialcopy is a tool for making a consistent copy of a specified subset of a database.

The primary use case of this tool is for developers who need production-like data for development. Using a copy of
production in development is often impossible due to the size of production as well as security and compliance concerns.

pg_partialcopy can filter the rows that are copied as well as transform the copied data to redact sensitive data.

## Installation

```
go install github.com/jackc/pg_partialcopy@latest
```

## Usage

```
pg_partialcopy <config-file>
```

Config file is a [TOML](https://toml.io/) file.

```toml
# source is the database from which data will be copied.
[source]
# database_url is a URL or key-value connection string. It is required.
database_url = "dbname=source"

# destination is the database to which data will be copied.
[destination]
# database_url is a URL or key-value connection string. It is required.
database_url = "dbname=destination"

# prepare_command is command(s) that will be run to prepare the destination database. It is run with the "sh" shell.
# Generally, it will optionally drop and create the empty destination database.
# prepare_command = "dropdb --if-exists destination && createdb destination"

# steps is an array of steps to execute.
[[steps]]
# table_name is the name of the table to copy. It is required.
table_name = "users"

# select_sql is a query that is used as the source for the copy. It can be used to filter or limit the rows returned as
# will as transform values (typically to redact values).
# select_sql = "select id, name, 'redacted' as email from users limit 100"

# select_sql, before_copy_sql, and after_copy_sql can be used for more advanced transformations such as using a temporary table.
[[steps]]
before_copy_sql = "create temporary table temp_people (like people)"`)

# table_name is always the name of the destination table.
table_name = "temp_people"

# If table_name is not the name of the source table then select_sql must be provided.
select_sql = "select * from people"

after_copy_sql = """
update temp_people set foo = 'bar';
insert into people select * from temp_people;
drop table temp_people;
"""

```


## Testing

The PostgreSQL command line programs `psql`, `dropdb`, and `createdb` must be in the PATH. A PostgreSQL server must be
running and set up such that those tools can be run without requiring any connection or authentication arguments. e.g.
`createdb foo` should work. The default user must be able to create and drop databases. You can use `PG*` variables such
as `PGHOST` and `PGUSER` if this must be configured.

```
go test
```
