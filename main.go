package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
)

var initFlag = flag.Bool("init", false, "Initialize config file")
var sourceURL = flag.String("source", "", "Source database URL or key-value connection string. Required if init is set.")
var destinationURL = flag.String("destination", "", "Destination database URL or key-value connection string")
var omitSelectSQL = flag.Bool("omitselectsql", false, "Omit select_sql from the config file")

func main() {
	flag.Usage = func() {
		fmt.Println("Usage: pg_partialcopy [options] <config-file>")
		fmt.Println("Options:")
		flag.PrintDefaults()
	}

	flag.Parse()

	if len(flag.Args()) == 0 {
		flag.Usage()
		fmt.Printf("\nError: config-file is required\n")
		return
	}

	if len(flag.Args()) > 1 {
		flag.Usage()
		fmt.Printf("\nError: unexpected arguments after config-file\n")
		return
	}
	configFilePath := flag.Arg(0)

	ctx := context.Background()

	if *initFlag {
		if *sourceURL == "" {
			flag.Usage()
			fmt.Printf("\nError: source is required when init is set\n")
			return
		}

		_, err := os.Stat(configFilePath)
		if err == nil {
			fmt.Printf("Error: config file %s already exists\n", configFilePath)
			return
		} else if !os.IsNotExist(err) {
			fmt.Printf("Error: unable to check config file %s: %v\n", configFilePath, err)
			return
		}

		err = initConfigFile(ctx, configFilePath, *sourceURL, *destinationURL, *omitSelectSQL)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		return
	}

	config, err := parseConfigFile(configFilePath)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	err = pgPartialCopy(ctx, config)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
