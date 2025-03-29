package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

const sourceDatabaseName = "pgsubset_test_source"
const sourceDatabaseURL = "dbname=" + sourceDatabaseName
const destinationDatabaseName = "pgsubset_test_destination"
const destinationDatabaseURL = "dbname=" + destinationDatabaseName
const setupSourceSQL = `drop table if exists a;
create table a (
	id int primary key
);
insert into a (id) values (1), (2), (3);

drop table if exists b;
create table b (
	id int primary key references a
);
insert into b (id) values (1), (2), (3);
`

func TestMain(m *testing.M) {
	handler := slog.NewTextHandler(os.NewFile(0, os.DevNull), nil)
	slog.SetDefault(slog.New(handler))

	err := exec.Command("dropdb", "--if-exists", sourceDatabaseName).Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error dropping source database:", err)
		os.Exit(1)
	}
	err = exec.Command("createdb", sourceDatabaseName).Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating source database:", err)
		os.Exit(1)
	}
	err = exec.Command("psql", "--no-psqlrc", "-c", setupSourceSQL, sourceDatabaseURL).Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error setting up source database:", err)
		os.Exit(1)
	}
	err = exec.Command("dropdb", "--if-exists", destinationDatabaseName).Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error dropping destination database:", err)
		os.Exit(1)
	}

	code := m.Run()
	os.Exit(code)
}

func parseAndRun(ctx context.Context, conf string) error {
	config, err := parseConfig(conf)
	if err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}

	err = pgsubset(ctx, config)
	if err != nil {
		return fmt.Errorf("error running pgsubset: %w", err)
	}

	return nil
}

func connectToDestination(t *testing.T) *pgconn.PgConn {
	destinationConn, err := pgconn.Connect(t.Context(), destinationDatabaseURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := destinationConn.Close(t.Context())
		require.NoError(t, err)
	})
	return destinationConn
}

func TestPgsubsetDefaultCopyAll(t *testing.T) {
	ctx := t.Context()
	err := parseAndRun(ctx, `[source]
database_url = "dbname=pgsubset_test_source"

[destination]
prepare_command = "dropdb --if-exists pgsubset_test_destination && createdb pgsubset_test_destination"
database_url = "dbname=pgsubset_test_destination"

[[steps]]
table_name = "a"`)
	require.NoError(t, err)

	destinationConn := connectToDestination(t)
	result := destinationConn.ExecParams(ctx, "select * from a order by id", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Equal(t, 3, len(result.Rows))
	require.Equal(t, "1", string(result.Rows[0][0]))
	require.Equal(t, "2", string(result.Rows[1][0]))
	require.Equal(t, "3", string(result.Rows[2][0]))
}

func TestPgsubsetSelectSQLFilterRows(t *testing.T) {
	ctx := t.Context()
	err := parseAndRun(ctx, `[source]
database_url = "dbname=pgsubset_test_source"

[destination]
prepare_command = "dropdb --if-exists pgsubset_test_destination && createdb pgsubset_test_destination"
database_url = "dbname=pgsubset_test_destination"

[[steps]]
table_name = "a"
select_sql = "select id from a where id > 1"`)
	require.NoError(t, err)

	destinationConn := connectToDestination(t)
	result := destinationConn.ExecParams(ctx, "select * from a order by id", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Equal(t, 2, len(result.Rows))
	require.Equal(t, "2", string(result.Rows[0][0]))
	require.Equal(t, "3", string(result.Rows[1][0]))
}

func TestPgsubsetSelectSQLTransformRows(t *testing.T) {
	ctx := t.Context()
	err := parseAndRun(ctx, `[source]
database_url = "dbname=pgsubset_test_source"

[destination]
prepare_command = "dropdb --if-exists pgsubset_test_destination && createdb pgsubset_test_destination"
database_url = "dbname=pgsubset_test_destination"

[[steps]]
table_name = "a"
select_sql = "select id*2 from a"`)
	require.NoError(t, err)

	destinationConn := connectToDestination(t)
	result := destinationConn.ExecParams(ctx, "select * from a order by id", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Equal(t, 3, len(result.Rows))
	require.Equal(t, "2", string(result.Rows[0][0]))
	require.Equal(t, "4", string(result.Rows[1][0]))
	require.Equal(t, "6", string(result.Rows[2][0]))
}

func TestPgsubsetForeignKeys(t *testing.T) {
	ctx := t.Context()

	// The step to copy b happens before the step to copy a, so a crash will occur unless the foreign key is removed.
	err := parseAndRun(ctx, `[source]
database_url = "dbname=pgsubset_test_source"

[destination]
prepare_command = "dropdb --if-exists pgsubset_test_destination && createdb pgsubset_test_destination"
database_url = "dbname=pgsubset_test_destination"

[[steps]]
table_name = "b"

[[steps]]
table_name = "a"`)
	require.NoError(t, err)

	destinationConn := connectToDestination(t)
	result := destinationConn.ExecParams(ctx, "select * from b order by id", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Equal(t, 3, len(result.Rows))
	require.Equal(t, "1", string(result.Rows[0][0]))
	require.Equal(t, "2", string(result.Rows[1][0]))
	require.Equal(t, "3", string(result.Rows[2][0]))

	result = destinationConn.ExecParams(ctx, "select * from a order by id", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Equal(t, 3, len(result.Rows))
	require.Equal(t, "1", string(result.Rows[0][0]))
	require.Equal(t, "2", string(result.Rows[1][0]))
	require.Equal(t, "3", string(result.Rows[2][0]))

	// Ensure the foreign key constraint has been restored.
	result = destinationConn.ExecParams(ctx, "insert into b (id) values (4)", nil, nil, nil, nil).Read()
	require.Error(t, result.Err)
	require.Contains(t, result.Err.Error(), "violates foreign key constraint")
}
