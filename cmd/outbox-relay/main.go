// Command outbox-relay is the reference relay binary. It blank-imports
// the lib-shipped plugins, loads a YAML address book, opens the DB,
// and runs the relay until it receives SIGINT/SIGTERM.
//
// Adopters who need a custom plugin maintain their own main that
// additionally blank-imports their plugin package(s):
//
//	import (
//	    _ "github.com/karolusz/outbox/publisher/gcppubsub"
//	    _ "company.com/internal/outbox-kafka-plugin"
//	)
//
// Everything else (flags, env vars, signal handling) can be reused.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"

	"github.com/karolusz/outbox/relay"
	"github.com/karolusz/outbox/yamlconfig"

	// Lib-shipped plugins. Adopters with custom plugins fork this file
	// and add their imports here.
	_ "github.com/karolusz/outbox/publisher/fake"
	_ "github.com/karolusz/outbox/publisher/gcppubsub"
)

func main() {
	addrBookPath := flag.String("addressbook", "/etc/outbox/addressbook.yaml",
		"path to the YAML address book")
	dbEnvVar := flag.String("db-env", "DB_CONNECTION_STRING",
		"env var holding the Postgres connection string")
	dbSchema := flag.String("schema", "outbox",
		"Postgres schema containing the messages table; usually 'outbox'")
	logLevel := flag.String("log-level", "info",
		"zerolog level: trace|debug|info|warn|error")
	flag.Parse()

	if err := run(*addrBookPath, *dbEnvVar, *dbSchema, *logLevel); err != nil {
		log.Fatal(err)
	}
}

func run(addrBookPath, dbEnvVar, dbSchema, logLevel string) error {
	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		return fmt.Errorf("parse log level: %w", err)
	}
	logger := zerolog.New(os.Stderr).Level(level).With().Timestamp().Logger()

	dbURL := os.Getenv(dbEnvVar)
	if dbURL == "" {
		return fmt.Errorf("%s env var not set", dbEnvVar)
	}

	db, err := sqlx.Open("postgres", dbURL)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	book, err := yamlconfig.LoadAddressBook(ctx, addrBookPath)
	if err != nil {
		return fmt.Errorf("load address book %s: %w", addrBookPath, err)
	}
	logger.Info().Str("path", addrBookPath).Msg("address book loaded")

	r := relay.New(db, &logger, book, nil, relay.WithDBSchema(dbSchema))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sig
		logger.Info().Str("signal", s.String()).Msg("shutdown signal received")
		cancel()
	}()

	logger.Info().Str("schema", dbSchema).Msg("relay starting")
	<-r.Start(ctx, nil)
	logger.Info().Msg("relay stopped")

	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("relay context: %w", err)
	}
	return nil
}
