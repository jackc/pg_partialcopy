package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"text/template"

	"github.com/BurntSushi/toml"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	Source      ConfigSource      `toml:"source"`
	Destination ConfigDestination `toml:"destination"`
	Steps       []*Step           `toml:"steps"`
}

type ConfigSource struct {
	DatabaseURL          string `toml:"database_url"`
	BeforeTransactionSQL string `toml:"before_transaction_sql"`
}

type ConfigDestination struct {
	PrepareCommand string `toml:"prepare_command"`
	DatabaseURL    string `toml:"database_url"`
}

type Step struct {
	TableName     string `toml:"table_name"`
	SelectSQL     string `toml:"select_sql"`
	BeforeCopySQL string `toml:"before_copy_sql"`
	AfterCopySQL  string `toml:"after_copy_sql"`
}

func initConfigFile(ctx context.Context, configFilePath, sourceURL, destinationURL string, omitSelectSQL bool) error {
	sourceConn, err := pgx.Connect(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("error connecting to source database: %w", err)
	}
	defer sourceConn.Close(ctx)

	selectSQLBuilder := &strings.Builder{}
	var sql string
	sql = `select
  quote_ident(table_schema) || '.' || quote_ident(table_name) as table_name,
	array_agg(quote_ident(column_name) order by columns.ordinal_position) as column_names
from information_schema.tables
  join information_schema.columns using(table_schema, table_name)
where table_schema not in ('information_schema', 'pg_catalog')
  and table_type = 'BASE TABLE'
group by table_schema, table_name
order by table_schema, table_name;`
	rows, _ := sourceConn.Query(ctx, sql)
	steps, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*Step, error) {
		var tableName string
		var columnNames []string
		err := row.Scan(&tableName, &columnNames)
		if err != nil {
			return nil, err
		}
		totalColumnNamesLen := 0
		for _, columnName := range columnNames {
			totalColumnNamesLen += len(columnName) + 2 // + 2 for ", "
		}

		if !omitSelectSQL {
			selectSQLBuilder.Reset()
			if totalColumnNamesLen > 114 {
				selectSQLBuilder.WriteString("select\n")
				for i, columnName := range columnNames {
					if i > 0 {
						selectSQLBuilder.WriteString(",\n")
					}
					selectSQLBuilder.WriteString("  ")
					selectSQLBuilder.WriteString(columnName)
				}
			} else {
				selectSQLBuilder.WriteString("select ")
				for i, columnName := range columnNames {
					if i > 0 {
						selectSQLBuilder.WriteString(", ")
					}
					selectSQLBuilder.WriteString(columnName)
				}
			}
			selectSQLBuilder.WriteString("\nfrom ")
			selectSQLBuilder.WriteString(tableName)
		}

		return &Step{
			TableName: tableName,
			SelectSQL: selectSQLBuilder.String(),
		}, nil
	})
	if err != nil {
		return fmt.Errorf("error executing SQL to get table names: %w", err)
	}

	file, err := os.Create(configFilePath)
	if err != nil {
		return fmt.Errorf("error creating config file: %w", err)
	}
	defer file.Close()

	tmpl := template.Must(template.New("config").Parse(`# source is the database from which data will be copied.
[source]
# database_url is a URL or key-value connection string. It is required.
database_url = {{.QuotedSourceURL}}

# before_transaction_sql is SQL that is run before the read-only transaction is started. A common use case would be to
# create a temporary table and populate it with data that will be used in steps with select_sql.
# before_transaction_sql = ""

# destination is the database to which data will be copied.
[destination]
# database_url is a URL or key-value connection string. It is required.
database_url = {{.QuotedDestinationURL}}

# prepare_command is command(s) that will be run to prepare the destination database. It is run with the "sh" shell.
# Generally, it will optionally drop and create the empty destination database.
# prepare_command = "dropdb --if-exists destination && createdb destination"

# steps is an array of steps to execute.
{{range .Steps -}}
[[steps]]
table_name = '{{.TableName}}'
{{if .SelectSQL -}}
select_sql = '''
{{.SelectSQL}}
'''
{{end}}
{{end -}}
`))

	sb := &strings.Builder{}
	err = toml.NewEncoder(sb).Encode(sourceURL)
	if err != nil {
		return fmt.Errorf("error encoding source URL: %w", err)
	}
	quotedSourceURL := sb.String()
	sb.Reset()
	err = toml.NewEncoder(sb).Encode(destinationURL)
	if err != nil {
		return fmt.Errorf("error encoding destination URL: %w", err)
	}
	quotedDestinationURL := sb.String()

	err = tmpl.Execute(file, struct {
		QuotedSourceURL      string
		QuotedDestinationURL string
		Steps                []*Step
	}{
		QuotedSourceURL:      quotedSourceURL,
		QuotedDestinationURL: quotedDestinationURL,
		Steps:                steps,
	})
	if err != nil {
		return fmt.Errorf("error writing config file: %w", err)
	}

	return nil
}

func parseConfigFile(configFilePath string) (*Config, error) {
	var config Config
	_, err := toml.DecodeFile(configFilePath, &config)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	return &config, nil
}

func parseConfig(s string) (*Config, error) {
	var config Config
	_, err := toml.Decode(s, &config)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	return &config, nil
}

func pgPartialCopy(ctx context.Context, config *Config) error {
	sourceConn, err := pgconn.Connect(ctx, config.Source.DatabaseURL)
	if err != nil {
		return fmt.Errorf("error connecting to source database: %w", err)
	}
	defer sourceConn.Close(ctx)

	if config.Source.BeforeTransactionSQL != "" {
		err := sourceConn.Exec(ctx, config.Source.BeforeTransactionSQL).Close()
		if err != nil {
			return fmt.Errorf("error executing before transaction SQL: %w", err)
		}
		slog.Info("Executed before transaction SQL")
	}

	result := sourceConn.ExecParams(ctx, "begin isolation level serializable read only deferrable", nil, nil, nil, nil).Read()
	if result.Err != nil {
		return fmt.Errorf("error starting transaction: %w", result.Err)
	}

	var snapshotID string
	result = sourceConn.ExecParams(ctx, "select pg_export_snapshot()", nil, nil, nil, nil).Read()
	if result.Err != nil {
		return fmt.Errorf("error exporting snapshot: %w", result.Err)
	}
	if len(result.Rows) != 1 {
		return fmt.Errorf("expected one row from pg_export_snapshot, got %d", len(result.Rows))
	}
	snapshotID = string(result.Rows[0][0])
	slog.Info("Began transaction on source", "snapshot_id", snapshotID)

	structureSQL, err := pgDumpStructureFromSource(config.Source.DatabaseURL, snapshotID)
	if err != nil {
		return fmt.Errorf("error dumping structure from source: %w", err)
	}
	slog.Info("Dumped structure from source")

	err = prepareDestination(config.Destination)
	if err != nil {
		return fmt.Errorf("error preparing destination: %w", err)
	}
	slog.Info("Prepared destination")

	err = loadStructureToDestination(config.Destination.DatabaseURL, structureSQL)
	if err != nil {
		return fmt.Errorf("error loading structure to destination: %w", err)
	}
	slog.Info("Loaded structure to destination")

	destinationConn, err := pgconn.Connect(ctx, config.Destination.DatabaseURL)
	if err != nil {
		return fmt.Errorf("error connecting to destination database: %w", err)
	}
	defer destinationConn.Close(ctx)

	recreateForeignKeyConstraintCommands, err := dropForeignKeyConstraints(ctx, destinationConn)
	if err != nil {
		return fmt.Errorf("error dropping foreign key constraints: %w", err)
	}
	slog.Info("Dropped foreign key constraints")

	for i, step := range config.Steps {
		err = executeStep(ctx, sourceConn, destinationConn, step)
		if err != nil {
			return fmt.Errorf("error executing step %d (%s): %w", i, step.TableName, err)
		}
		slog.Info("Executed step", "idx", i, "table_name", step.TableName)
	}

	err = recreateForeignKeyConstraints(ctx, destinationConn, recreateForeignKeyConstraintCommands)
	if err != nil {
		return fmt.Errorf("error recreating foreign key constraints: %w", err)
	}
	slog.Info("Recreated foreign key constraints")

	return nil
}

func pgDumpStructureFromSource(databaseURL, snapshotID string) ([]byte, error) {
	return exec.Command("pg_dump",
		"--snapshot", snapshotID,
		"--schema-only",
		"--no-owner",
		"--no-privileges",
		databaseURL,
	).Output()
}

func prepareDestination(configDestination ConfigDestination) error {
	if configDestination.PrepareCommand == "" {
		return nil
	}

	return exec.Command("sh", "-c", configDestination.PrepareCommand).Run()
}

func loadStructureToDestination(databaseURL string, structureSQL []byte) error {
	cmd := exec.Command("psql", "--no-psqlrc", databaseURL)
	cmd.Stdin = bytes.NewReader(structureSQL)
	return cmd.Run()
}

func dropForeignKeyConstraints(ctx context.Context, conn *pgconn.PgConn) ([]string, error) {
	result := conn.ExecParams(
		ctx,
		"select conrelid::regclass as table_name, conname as constraint_name, pg_get_constraintdef(oid) constraint_definition from pg_constraint where contype = 'f'",
		nil, nil, nil, nil,
	).Read()
	if result.Err != nil {
		return nil, result.Err
	}

	createForeignKeyConstraintCommands := make([]string, 0, len(result.Rows))

	for _, row := range result.Rows {
		tableName := string(row[0])
		constraintName := string(row[1])
		constraintDefinition := string(row[2])

		dropConstraintSQL := fmt.Sprintf("alter table %s drop constraint %s", tableName, constraintName)
		result = conn.ExecParams(ctx, dropConstraintSQL, nil, nil, nil, nil).Read()
		if result.Err != nil {
			return nil, result.Err
		}

		createForeignKeyConstraintCommands = append(createForeignKeyConstraintCommands, fmt.Sprintf("alter table %s add constraint %s %s", tableName, constraintName, constraintDefinition))
	}

	return createForeignKeyConstraintCommands, nil
}

func recreateForeignKeyConstraints(ctx context.Context, conn *pgconn.PgConn, commands []string) error {
	for _, cmd := range commands {
		result := conn.ExecParams(ctx, cmd, nil, nil, nil, nil).Read()
		if result.Err != nil {
			return result.Err
		}
	}
	return nil
}

func executeStep(ctx context.Context, sourceConn, destinationConn *pgconn.PgConn, step *Step) error {
	if step.BeforeCopySQL != "" {
		err := destinationConn.Exec(ctx, step.BeforeCopySQL).Close()
		if err != nil {
			return fmt.Errorf("error executing before copy SQL: %w", err)
		}
	}

	r, w := io.Pipe()
	g := &errgroup.Group{}
	g.Go(func() error {
		defer w.Close()

		var copyToSQL string
		if step.SelectSQL != "" {
			copyToSQL = fmt.Sprintf("copy (%s) to stdout", step.SelectSQL)
		} else {
			copyToSQL = fmt.Sprintf("copy %s to stdout", step.TableName)
		}
		_, err := sourceConn.CopyTo(ctx, w, copyToSQL)
		if err != nil {
			w.CloseWithError(err)
			return err
		}

		return nil
	})

	g.Go(func() error {
		copyFromSQL := fmt.Sprintf("copy %s from stdin", step.TableName)
		_, err := destinationConn.CopyFrom(ctx, r, copyFromSQL)
		if err != nil {
			r.CloseWithError(err)
			return err
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if step.AfterCopySQL != "" {
		err := destinationConn.Exec(ctx, step.AfterCopySQL).Close()
		if err != nil {
			return fmt.Errorf("error executing after copy SQL: %w", err)
		}
	}

	return nil
}
