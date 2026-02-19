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
	ensureSettingsKV(db)
	ensureRulesetRemoval(db)

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func ensureRulesetRemoval(db *sqlx.DB) {
	if !tableExists(db, "rulesets") && !tableHasColumn(db, "games", "ruleset_id") && !tableHasColumn(db, "matchups", "ruleset_id") {
		return
	}

	if tableHasColumn(db, "games", "ruleset_id") {
		db.MustExec(`
			CREATE TABLE games_new (
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
			);
		`)
		if tableExists(db, "rulesets") {
			db.MustExec(`
				INSERT INTO games_new (id, played_at, white_player_id, black_player_id, movetime_ms, book_path, result, termination, moves_uci, book_plies)
				SELECT g.id,
					g.played_at,
					g.white_player_id,
					g.black_player_id,
					COALESCE(r.movetime_ms, 0),
					COALESCE(r.book_path, ''),
					g.result,
					g.termination,
					g.moves_uci,
					g.book_plies
				FROM games g
				LEFT JOIN rulesets r ON g.ruleset_id = r.id
			`)
		} else {
			db.MustExec(`
				INSERT INTO games_new (id, played_at, white_player_id, black_player_id, movetime_ms, book_path, result, termination, moves_uci, book_plies)
				SELECT id,
					played_at,
					white_player_id,
					black_player_id,
					0,
					'',
					result,
					termination,
					moves_uci,
					book_plies
				FROM games
			`)
		}
		db.MustExec(`DROP TABLE games`)
		db.MustExec(`ALTER TABLE games_new RENAME TO games`)
	}

	if tableHasColumn(db, "matchups", "ruleset_id") {
		db.MustExec(`
			CREATE TABLE matchups_new (
				id INTEGER PRIMARY KEY,
				player_a_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
				player_b_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
				UNIQUE(player_a_id, player_b_id)
			);
		`)
		db.MustExec(`
			INSERT OR IGNORE INTO matchups_new (player_a_id, player_b_id)
			SELECT DISTINCT player_a_id, player_b_id FROM matchups
		`)
		db.MustExec(`DROP TABLE matchups`)
		db.MustExec(`ALTER TABLE matchups_new RENAME TO matchups`)
	}

	if tableExists(db, "rulesets") {
		var defaults struct {
			MovetimeMS int    `db:"movetime_ms"`
			BookPath   string `db:"book_path"`
		}
		if err := db.Get(&defaults, `SELECT movetime_ms, book_path FROM rulesets ORDER BY id ASC LIMIT 1`); err == nil {
			db.MustExec(`INSERT OR REPLACE INTO settings (key, value) VALUES ('game_movetime_ms', ?)`, defaults.MovetimeMS)
			db.MustExec(`INSERT OR REPLACE INTO settings (key, value) VALUES ('game_book_path', ?)`, defaults.BookPath)
		}
		db.MustExec(`DROP TABLE rulesets`)
	}

	insertDefaultSettings(db)
	if tableHasColumn(db, "games", "book_path") {
		db.MustExec(`UPDATE games SET book_path = '' WHERE book_path IS NULL`)
	}
	db.MustExec(`CREATE INDEX IF NOT EXISTS idx_games_played_at ON games(played_at)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS idx_games_white_player_id ON games(white_player_id)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS idx_games_black_player_id ON games(black_player_id)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS idx_games_matchup ON games(white_player_id, black_player_id)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS idx_matchups_player_a_id ON matchups(player_a_id)`)
	db.MustExec(`CREATE INDEX IF NOT EXISTS idx_matchups_player_b_id ON matchups(player_b_id)`)
}

func tableExists(db *sqlx.DB, name string) bool {
	var found int
	if err := db.Get(&found, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`, name); err != nil {
		return false
	}
	return found > 0
}

func tableHasColumn(db *sqlx.DB, table, column string) bool {
	var cols []struct {
		Name string `db:"name"`
	}
	query := fmt.Sprintf("SELECT name FROM pragma_table_info('%s')", table)
	if err := db.Select(&cols, query); err != nil {
		return false
	}
	for _, col := range cols {
		if col.Name == column {
			return true
		}
	}
	return false
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
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('game_movetime_ms', 100)`)
	db.MustExec(`INSERT OR IGNORE INTO settings (key, value) VALUES ('game_book_path', '')`)
}

/*func mustExec(db *sqlx.DB, query string, args ...any) {
	if _, err := db.Exec(query, args...); err != nil {
		panic(fmt.Errorf("db init exec failed: %w", err))
	}
}*/
