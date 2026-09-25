package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
)

// admin is a connection to the server's maintenance database, used to create
// each run's database and read server-wide statistics.
type admin struct {
	conn    *pgx.Conn
	baseURL *url.URL
	// statements is whether pg_stat_statements is loaded.
	statements bool
}

func connectAdmin(ctx context.Context, rawURL string) (*admin, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("database URL: %w", err)
	}
	conn, err := pgx.Connect(ctx, rawURL)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", u.Redacted(), err)
	}
	_, err = conn.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
	return &admin{conn: conn, baseURL: u, statements: err == nil}, nil
}

// createDatabase makes an empty database and returns its URL.
func (a *admin) createDatabase(ctx context.Context, name string) (string, error) {
	if _, err := a.conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
		return "", err
	}
	if _, err := a.conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		return "", err
	}
	u := *a.baseURL
	u.Path = "/" + name
	return u.String(), nil
}

func (a *admin) dropDatabase(ctx context.Context, name string) error {
	_, err := a.conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	return err
}

// dbStats are cumulative counters for one database, plus the server's WAL
// position.
type dbStats struct {
	XactCommit   int64
	XactRollback int64
	TupInserted  int64
	TupUpdated   int64
	TupDeleted   int64
	TupFetched   int64
	BlocksRead   int64
	BlocksHit    int64
	WALBytes     float64
	// Statement totals from pg_stat_statements; zero when it is not loaded.
	Calls       int64
	ExecMs      float64
	StatementOK bool
}

func (a *admin) snapshot(ctx context.Context, db string) (dbStats, error) {
	var s dbStats
	err := a.conn.QueryRow(ctx, `
		SELECT xact_commit, xact_rollback, tup_inserted, tup_updated, tup_deleted,
		       tup_fetched, blks_read, blks_hit,
		       pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::float8
		FROM pg_stat_database WHERE datname = $1`, db).Scan(
		&s.XactCommit, &s.XactRollback, &s.TupInserted, &s.TupUpdated, &s.TupDeleted,
		&s.TupFetched, &s.BlocksRead, &s.BlocksHit, &s.WALBytes)
	if err != nil {
		return s, fmt.Errorf("pg_stat_database: %w", err)
	}
	if !a.statements {
		return s, nil
	}
	err = a.conn.QueryRow(ctx, `
		SELECT coalesce(sum(calls), 0)::int8, coalesce(sum(total_exec_time), 0)::float8
		FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid
		WHERE d.datname = $1`, db).Scan(&s.Calls, &s.ExecMs)
	if err != nil {
		return s, fmt.Errorf("pg_stat_statements: %w", err)
	}
	s.StatementOK = true
	return s, nil
}

func (s dbStats) minus(before dbStats) dbStats {
	return dbStats{
		XactCommit:   s.XactCommit - before.XactCommit,
		XactRollback: s.XactRollback - before.XactRollback,
		TupInserted:  s.TupInserted - before.TupInserted,
		TupUpdated:   s.TupUpdated - before.TupUpdated,
		TupDeleted:   s.TupDeleted - before.TupDeleted,
		TupFetched:   s.TupFetched - before.TupFetched,
		BlocksRead:   s.BlocksRead - before.BlocksRead,
		BlocksHit:    s.BlocksHit - before.BlocksHit,
		WALBytes:     s.WALBytes - before.WALBytes,
		Calls:        s.Calls - before.Calls,
		ExecMs:       s.ExecMs - before.ExecMs,
		StatementOK:  s.StatementOK && before.StatementOK,
	}
}
