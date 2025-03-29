# pgsubset

pgsubset is a tool for making a consistent copy of a specified subset of a database.

The primary use case of this tool is for developers who need production-like data for development. Using a copy of production in development is often impossible due to the size of production as well as security and compliance concerns.

pgsubset can filter the rows that are copied as well as transform the copied data to redact sensitive data.

## Installation

```
go install github.com/jackc/pgsubset@latest
```

## Testing

The PostgreSQL command line programs `psql`, `dropdb`, and `createdb` must be in the PATH. A PostgreSQL server must be
running and set up such that those tools can be run without requiring any connection or authentication arguments. e.g.
`createdb foo` should work. The default user must be able to create and drop databases. You can use `PG*` variables such
as `PGHOST` and `PGUSER` if this must be configured.

```
go test
```
