package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"

	"github.com/BurntSushi/toml"
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
			return err
		}

		return nil
	})

	g.Go(func() error {
		copyFromSQL := fmt.Sprintf("copy %s from stdin", step.TableName)
		_, err := destinationConn.CopyFrom(ctx, r, copyFromSQL)
		if err != nil {
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
