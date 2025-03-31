package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		printUsage()
		return
	}
	configFilePath := os.Args[1]

	config, err := parseConfigFile(configFilePath)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	ctx := context.Background()
	err = pgPartialCopy(ctx, config)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: pg_partialcopy <config-file>")
}
