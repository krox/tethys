package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

// note: as per SQLites's manual suggestions, we do not use 'AUTOINCREMENT' on
// the 'INTEGER PRIMARY KEY' columns. The default behaviour of such columns is
// nearly identical anyway, with less overhead.
var schema_stmts = []string{
	`PRAGMA journal_mode=WAL;`,
	`PRAGMA foreign_keys=ON;`,
	`CREATE TABLE IF NOT EXISTS players (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		engine_path TEXT NOT NULL DEFAULT '',
		engine_args TEXT NOT NULL DEFAULT '',
		engine_init TEXT NOT NULL DEFAULT '',
		engine_elo REAL NOT NULL DEFAULT 0,
		UNIQUE(name)
	);`,
	`CREATE TABLE IF NOT EXISTS matchups (
		id INTEGER PRIMARY KEY,
		player_a_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		player_b_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		UNIQUE(player_a_id, player_b_id)
	);`,
	`CREATE TABLE IF NOT EXISTS games (
		id INTEGER PRIMARY KEY,
		played_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
		white_player_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		black_player_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		movetime_ms INTEGER NOT NULL DEFAULT 0,
		book_path TEXT NOT NULL DEFAULT '',
		result TEXT NOT NULL DEFAULT '',
		termination TEXT NOT NULL DEFAULT '',
		moves_uci TEXT NOT NULL DEFAULT '',
		ply_count INTEGER NOT NULL GENERATED ALWAYS AS (length(moves_uci) - length(replace(moves_uci, ' ', '')) + CASE WHEN moves_uci = '' THEN 0 ELSE 1 END) STORED,
		book_plies INTEGER NOT NULL DEFAULT 0
		CHECK (result IN ('', '1-0', '0-1', '1/2-1/2'))
		CHECK (trim(moves_uci) = moves_uci)
	);`,
	`CREATE TABLE IF NOT EXISTS evals (
		zobrist_key INTEGER PRIMARY KEY,
		fen TEXT NOT NULL,
		score TEXT NOT NULL DEFAULT '',
		pv TEXT NOT NULL DEFAULT '',
		engine_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		depth INTEGER NOT NULL DEFAULT 0
	);`,
	`CREATE TABLE IF NOT EXISTS settings (
		key TEXT PRIMARY KEY,
		value
	);`,
	`UPDATE players SET engine_path = '' WHERE engine_path IS NULL;`,
	`UPDATE games SET result = '' WHERE result IS NULL;`,
	`UPDATE games SET termination = '' WHERE termination IS NULL;`,
	`CREATE INDEX IF NOT EXISTS idx_games_played_at ON games(played_at);`,
	`CREATE INDEX IF NOT EXISTS idx_games_white_player_id ON games(white_player_id);`,
	`CREATE INDEX IF NOT EXISTS idx_games_black_player_id ON games(black_player_id);`,
	`CREATE INDEX IF NOT EXISTS idx_games_matchup ON games(white_player_id, black_player_id);`,
	`CREATE INDEX IF NOT EXISTS idx_evals_engine_id ON evals(engine_id);`,
	`CREATE INDEX IF NOT EXISTS idx_matchups_player_a_id ON matchups(player_a_id);`,
	`CREATE INDEX IF NOT EXISTS idx_matchups_player_b_id ON matchups(player_b_id);`,
}

type Store struct {
	db *sqlx.DB
}

func Open(path string) (*Store, error) {
	db, err := sqlx.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// keep it predictable; this is a single-instance service.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	for _, stmt := range schema_stmts {
		db.MustExec(stmt)
	}
	insertDefaultSettings(db)

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func insertDefaultSettings(db *sqlx.DB) {
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('opening_min', 20)`)
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('analysis_engine_id', 0)`)
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('analysis_depth', 12)`)
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('game_movetime_ms', 100)`)
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('game_book_path', '')`)
}
