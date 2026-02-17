package db

import (
	"context"
	"database/sql"
	"errors"
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
	`CREATE TABLE IF NOT EXISTS rulesets (
		id INTEGER PRIMARY KEY,
		movetime_ms INTEGER NOT NULL DEFAULT 100,
		book_path TEXT NOT NULL DEFAULT '',
		book_max_plies INTEGER NOT NULL DEFAULT 0,
		UNIQUE(movetime_ms, book_path, book_max_plies)
	);`,
	`CREATE TABLE IF NOT EXISTS matchups (
		id INTEGER PRIMARY KEY,
		player_a_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		player_b_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		ruleset_id INTEGER NOT NULL REFERENCES rulesets(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		UNIQUE(player_a_id, player_b_id, ruleset_id)
	);`,
	`CREATE TABLE IF NOT EXISTS games (
		id INTEGER PRIMARY KEY,
		played_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
		white_player_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		black_player_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		ruleset_id INTEGER NOT NULL REFERENCES rulesets(id) ON UPDATE CASCADE ON DELETE RESTRICT,
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
	`UPDATE rulesets SET book_path = '' WHERE book_path IS NULL;`,
	`UPDATE games SET result = '' WHERE result IS NULL;`,
	`UPDATE games SET termination = '' WHERE termination IS NULL;`,
	`CREATE INDEX IF NOT EXISTS idx_games_played_at ON games(played_at);`,
	`CREATE INDEX IF NOT EXISTS idx_games_white_player_id ON games(white_player_id);`,
	`CREATE INDEX IF NOT EXISTS idx_games_black_player_id ON games(black_player_id);`,
	`CREATE INDEX IF NOT EXISTS idx_games_matchup ON games(white_player_id, black_player_id, ruleset_id);`,
	`CREATE INDEX IF NOT EXISTS idx_evals_engine_id ON evals(engine_id);`,
	`CREATE INDEX IF NOT EXISTS idx_matchups_player_a_id ON matchups(player_a_id);`,
	`CREATE INDEX IF NOT EXISTS idx_matchups_player_b_id ON matchups(player_b_id);`,
	`CREATE INDEX IF NOT EXISTS idx_matchups_ruleset_id ON matchups(ruleset_id);`,
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
	ensureDefaultRulesetExists(db)
	ensureSettingsKV(db)

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func ensureDefaultRulesetExists(db *sqlx.DB) {
	var id int64
	if err := db.Get(&id, `SELECT id FROM rulesets ORDER BY id ASC LIMIT 1`); err == nil {
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		panic(fmt.Errorf("default ruleset lookup: %w", err))
	}
	db.MustExec(`
		INSERT INTO rulesets (movetime_ms, book_path, book_max_plies)
		VALUES (100, '', 0)
	`)
}

func ensureSettingsKV(db *sqlx.DB) {
	type tableInfo struct {
		Name string `db:"name"`
	}
	var cols []tableInfo
	if err := db.Select(&cols, `SELECT name FROM pragma_table_info('settings')`); err != nil {
		panic(fmt.Errorf("settings table info: %w", err))
	}
	if len(cols) == 0 {
		db.MustExec(`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value)`)
		insertDefaultSettings(db)
		return
	}

	hasKey := false
	hasValue := false
	hasLegacy := false
	for _, col := range cols {
		switch col.Name {
		case "key":
			hasKey = true
		case "value":
			hasValue = true
		case "id", "opening_min", "analysis_engine_id", "analysis_depth":
			hasLegacy = true
		}
	}

	if hasKey && hasValue {
		insertDefaultSettings(db)
		return
	}
	if !hasLegacy {
		db.MustExec(`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value)`)
		insertDefaultSettings(db)
		return
	}

	db.MustExec(`CREATE TABLE settings_new (key TEXT PRIMARY KEY, value)`)
	db.MustExec(`INSERT OR IGNORE INTO settings_new (key, value) SELECT 'opening_min', opening_min FROM settings WHERE id = 1`)
	db.MustExec(`INSERT OR IGNORE INTO settings_new (key, value) SELECT 'analysis_engine_id', analysis_engine_id FROM settings WHERE id = 1`)
	db.MustExec(`INSERT OR IGNORE INTO settings_new (key, value) SELECT 'analysis_depth', analysis_depth FROM settings WHERE id = 1`)
	db.MustExec(`DROP TABLE settings`)
	db.MustExec(`ALTER TABLE settings_new RENAME TO settings`)
	insertDefaultSettings(db)
}

func insertDefaultSettings(db *sqlx.DB) {
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('opening_min', 20)`)
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('analysis_engine_id', 0)`)
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('analysis_depth', 12)`)
}

/*func mustExec(db *sqlx.DB, query string, args ...any) {
	if _, err := db.Exec(query, args...); err != nil {
		panic(fmt.Errorf("db init exec failed: %w", err))
	}
}*/
