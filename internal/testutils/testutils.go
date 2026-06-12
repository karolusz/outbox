// Package testutils provides a minimal set of helpers used by the outbox
// test suite. Lifted (slimmed) from direct-debit-engine's internal/testutils.
package testutils

import (
	"database/sql"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// SetupMockDB returns a sqlx-wrapped sqlmock and a cleanup func.
func SetupMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	cleanup := func() { sqlxDB.Close() }
	return sqlxDB, mock, cleanup
}

// NewTestLogger returns a console-formatted zerolog logger at Debug level.
func NewTestLogger(t *testing.T) zerolog.Logger {
	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).
		Level(zerolog.DebugLevel).
		With().
		Timestamp().
		Logger()
}

// NoGoroutineLeak ensures the calling test does not leave goroutines running
// after it returns. Used via:
//
//	defer testutils.NoGoroutineLeak(t)
func NoGoroutineLeak(t *testing.T) {
	t.Helper()
	goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
		goleak.IgnoreTopFunction("testing.(*T).Run"),
	)
}

// SeedTables executes a SQL file from the provided FS against the DB.
// Pass an empty seedScriptFileName to skip seeding.
func SeedTables(t *testing.T, db *sql.DB, sqlFS fs.FS, seedScriptFileName string) {
	t.Helper()
	if seedScriptFileName == "" {
		return
	}
	bytes, err := fs.ReadFile(sqlFS, seedScriptFileName)
	if err != nil {
		t.Fatalf("read %s: %v", seedScriptFileName, err)
	}
	if _, err := db.Exec(string(bytes)); err != nil {
		t.Fatalf("exec %s: %v", seedScriptFileName, err)
	}
}

// NewTestDB creates an isolated test database by cloning the template
// database referenced in dbConnStr. The new database is named
// "{template}_{testName}". The returned cleanup func drops it.
//
// Intended for integration tests; expects a running Postgres with the
// outbox migrations already applied to the template DB.
func NewTestDB(dbConnStr, testName string, maxPool, maxIdle int) (*sqlx.DB, func(), error) {
	templateDB, adminConnStr, err := parseConnStr(dbConnStr)
	if err != nil {
		return nil, nil, fmt.Errorf("parse connstr: %w", err)
	}

	testDBName := sanitizeDBName(templateDB + "_" + testName)

	adminDB, err := sqlx.Connect("postgres", adminConnStr)
	if err != nil {
		return nil, nil, fmt.Errorf("connect admin: %w", err)
	}

	_, _ = adminDB.Exec(fmt.Sprintf(`
		SELECT pg_terminate_backend(pg_stat_activity.pid)
		FROM pg_stat_activity
		WHERE pg_stat_activity.datname = '%s'
		AND pid <> pg_backend_pid()`, testDBName))
	_, _ = adminDB.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, testDBName))

	if _, err = adminDB.Exec(fmt.Sprintf(`CREATE DATABASE %s TEMPLATE %s`, testDBName, templateDB)); err != nil {
		_ = adminDB.Close()
		return nil, nil, fmt.Errorf("create test db: %w", err)
	}

	testConnStr := replaceDBNameInConnStr(dbConnStr, templateDB, testDBName)
	testDB, err := sqlx.Connect("postgres", testConnStr)
	if err != nil {
		_, _ = adminDB.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, testDBName))
		_ = adminDB.Close()
		return nil, nil, fmt.Errorf("connect test db: %w", err)
	}

	testDB.SetMaxOpenConns(maxPool)
	testDB.SetMaxIdleConns(maxIdle)

	if err := truncateAllTables(testDB); err != nil {
		_ = testDB.Close()
		_, _ = adminDB.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, testDBName))
		_ = adminDB.Close()
		return nil, nil, fmt.Errorf("truncate: %w", err)
	}

	cleanup := func() {
		_ = testDB.Close()
		_, _ = adminDB.Exec(fmt.Sprintf(`
			SELECT pg_terminate_backend(pg_stat_activity.pid)
			FROM pg_stat_activity
			WHERE pg_stat_activity.datname = '%s'
			AND pid <> pg_backend_pid()`, testDBName))
		_, _ = adminDB.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, testDBName))
		_ = adminDB.Close()
	}

	return testDB, cleanup, nil
}

func parseConnStr(connStr string) (string, string, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", "", fmt.Errorf("invalid connstr: %w", err)
	}
	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" {
		return "", "", fmt.Errorf("no database name in connstr")
	}
	adminURL := *u
	adminURL.Path = "/postgres"
	return dbName, adminURL.String(), nil
}

func truncateAllTables(db *sqlx.DB) error {
	rows, err := db.Query(`
		SELECT tablename FROM pg_tables
		WHERE schemaname = 'public' AND tablename NOT LIKE 'goose_%'`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return err
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	_, err = db.Exec(fmt.Sprintf("TRUNCATE TABLE %s CASCADE", strings.Join(tables, ", ")))
	return err
}

func replaceDBNameInConnStr(connStr, oldDB, newDB string) string {
	u, err := url.Parse(connStr)
	if err != nil {
		return strings.Replace(connStr, "/"+oldDB, "/"+newDB, 1)
	}
	u.Path = "/" + newDB
	return u.String()
}

func sanitizeDBName(name string) string {
	name = strings.ToLower(name)
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	name = b.String()
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}
