[![Build Status](https://github.com/jackc/pg_partialcopy/actions/workflows/ci.yml/badge.svg)](https://github.com/jackc/pg_partialcopy/actions/workflows/ci.yml)

# pg_partialcopy

`pg_partialcopy` is a tool for making a consistent copy of a specified subset of a database.

The primary use case of this tool is for developers who need production-like data for development. Using a copy of
production in development is often impossible due to the size of production as well as security and compliance concerns.

`pg_partialcopy` can filter the rows that are copied as well as transform the copied data to redact sensitive data.

## Installation

```
go install github.com/jackc/pg_partialcopy@latest
```

## Usage

To create a initialize a new config file:

```
pg_partialcopy -init -source='service=your-prod-service' -destination='dbname=localcopy' yourconfig.toml
```

`source` and `destination` can be a database URL or a key-value connection string.

This command will connect to your source database, inspect it, and create a config file that will backup every row of every table.

By default, each table has a `select_sql` option that specifies each column to be included. This ensures that if a
column is added to a source database, it is not automatically included. This is important to ensure sensitive data is
not inadvertently copied. However, you can use the `omitselectsql` option to omit the `select_sql` configuration for a
more concise configuration file.

The `destination.prepare_command` should usually be configured before a copy is performed. Typically, it will drop and recreate the destination database, but this must be configured manually. e.g.

```
prepare_command = "dropdb --if-exists my_copy && createdb my_copy"
```

To perform the partial copy:

```
pg_partialcopy yourconfig.toml
```

Config file is a [TOML](https://toml.io/) file.

```toml
# source is the database from which data will be copied.
[source]
# database_url is a URL or key-value connection string. It is required.
database_url = "dbname=source"

# before_transaction_sql is SQL that is run before the read-only transaction is started. A common use case would be to
# create a temporary table and populate it with data that will be used in steps with select_sql.
before_transaction_sql = """
create temporary table selected_people (id uuid)
select id
from people tablesample bernoulli(10)
"""

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
# will as transform values (typically to redact values). It is used as part of a copy command so it must not have a
# semicolon at the end.
# select_sql = "select id, name, 'redacted' as email from users limit 100"

# select_sql, before_copy_sql, and after_copy_sql can be used for more advanced transformations such as using a temporary table.
[[steps]]
before_copy_sql = "create temporary table temp_people (like people)"`)

# table_name is always the name of the destination table.
table_name = "temp_people"

# If table_name is not the name of the source table then select_sql must be provided.
select_sql = "select p.* from people p join selected_people sp using(id)"

after_copy_sql = """
update temp_people set foo = 'bar';
insert into people select * from temp_people;
drop table temp_people;
"""
```

Config files are processed through [text/template](https://pkg.go.dev/text/template). [sprout](https://github.com/go-sprout/sprout) functions from the `std`, `env`, `maps`, `slices`, and `strings` repositories are available.

This would most commonly be used to insert environment variables into a config file. e.g.

```toml
[destination]
# database_url is a URL or key-value connection string. It is required.
database_url = "dbname={{env "DESTDB"}}"
```

## How It Works

1. Establish connection to source database.
2. Execute source.before_transaction_sql. This is typically used to store the IDs of selected records when they must be referenced in multiple steps.
3. Begin a serializable read only deferrable transaction. This type of transaction is guaranteed to not block any other connections and to get a consistent snapshot.
4. Use `pg_export_snapshot()` to get the snapshot ID.
5. Call `pg_dump` with the snapshot ID and dump the structure of the source database.
6. Execute `destination.prepare_command` with `sh`.
7. Load the structure from the source into the destination.
8. Drop foreign key constraints.
9. Execute each step.
10. Recreate foreign key constraints.

For each step:

1. Execute `before_copy_sql` on the destination.
2. Use the `COPY` protocol to copy data from the source to the destination.
3. Execute `after_copy_sql` on the destination.



## Testing

The PostgreSQL command line programs `psql`, `dropdb`, and `createdb` must be in the PATH. A PostgreSQL server must be
running and set up such that those tools can be run without requiring any connection or authentication arguments. e.g.
`createdb foo` should work. The default user must be able to create and drop databases. You can use `PG*` variables such
as `PGHOST` and `PGUSER` if this must be configured.

```
go test
```
