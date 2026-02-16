package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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
		engine_path TEXT NULL,
		engine_args TEXT NOT NULL DEFAULT '',
		engine_init TEXT NOT NULL DEFAULT '',
		engine_elo REAL NOT NULL DEFAULT 0,
		UNIQUE(name)
	);`,
	`CREATE TABLE IF NOT EXISTS rulesets (
		id INTEGER PRIMARY KEY,
		movetime_ms INTEGER NOT NULL DEFAULT 100,
		book_path TEXT NULL DEFAULT NULL,
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
		result TEXT,         -- 1-0|0-1|1/2-1/2 (or NULL)
		termination TEXT,
		moves_uci TEXT NOT NULL DEFAULT '',
		ply_count INTEGER NOT NULL GENERATED ALWAYS AS (length(moves_uci) - length(replace(moves_uci, ' ', '')) + CASE WHEN moves_uci = '' THEN 0 ELSE 1 END) STORED,
		book_plies INTEGER NOT NULL DEFAULT 0
		CHECK (result IS NULL OR result IN ('1-0', '0-1', '1/2-1/2'))
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
		id INTEGER PRIMARY KEY CHECK (id = 1),
		opening_min INTEGER NOT NULL DEFAULT 20,
		analysis_engine_id INTEGER NOT NULL DEFAULT 0,
		analysis_depth INTEGER NOT NULL DEFAULT 12,
		updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	);`,
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
		mustExec(db, stmt)
	}
	ensureDefaultRulesetExists(db)
	mustExec(db, `INSERT OR IGNORE INTO settings (id) VALUES (1)`)

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

type Settings struct {
	OpeningMin       int   `db:"opening_min"`
	AnalysisEngineID int64 `db:"analysis_engine_id"`
	AnalysisDepth    int   `db:"analysis_depth"`
}

func (s *Store) GetSettings(ctx context.Context) (Settings, error) {
	var row Settings
	if err := s.db.GetContext(ctx, &row, `
		SELECT opening_min, analysis_engine_id, analysis_depth
		FROM settings
		WHERE id = 1
	`); err != nil {
		return Settings{}, err
	}
	return row, nil
}

func (s *Store) UpdateSettings(ctx context.Context, settings Settings) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE settings
		SET opening_min = ?,
			analysis_engine_id = ?,
			analysis_depth = ?,
			updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		WHERE id = 1
	`, settings.OpeningMin, settings.AnalysisEngineID, settings.AnalysisDepth)
	return err
}

func ensureDefaultRulesetExists(db *sqlx.DB) {
	var id int64
	if err := db.Get(&id, `SELECT id FROM rulesets ORDER BY id ASC LIMIT 1`); err == nil {
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		panic(fmt.Errorf("default ruleset lookup: %w", err))
	}
	mustExec(db, `
		INSERT INTO rulesets (movetime_ms, book_path, book_max_plies)
		VALUES (100, NULL, 0)
	`)
}

func mustExec(db *sqlx.DB, query string, args ...any) {
	if _, err := db.Exec(query, args...); err != nil {
		panic(fmt.Errorf("db init exec failed: %w", err))
	}
}

type GameDetail struct {
	ID          int64  `db:"id"`
	PlayedAt    string `db:"played_at"`
	White       string `db:"white"`
	Black       string `db:"black"`
	MovetimeMS  int    `db:"movetime_ms"`
	Result      string `db:"result"`
	Termination string `db:"termination"`
	MovesUCI    string `db:"moves_uci"`
	Plies       int    `db:"ply_count"`
	BookPlies   int    `db:"book_plies"`
}

type Eval struct {
	ZobristKey uint64 `db:"zobrist_key"`
	FEN        string `db:"fen"`
	Score      string `db:"score"`
	PV         string `db:"pv"`
	EngineID   int64  `db:"engine_id"`
	Depth      int    `db:"depth"`
}

type Engine struct {
	ID   int64   `db:"id"`
	Name string  `db:"name"`
	Path string  `db:"engine_path"`
	Args string  `db:"engine_args"`
	Init string  `db:"engine_init"`
	Elo  float64 `db:"engine_elo"`
}

type Matchup struct {
	ID        int64 `db:"id"`
	PlayerAID int64 `db:"player_a_id"`
	PlayerBID int64 `db:"player_b_id"`
	RulesetID int64 `db:"ruleset_id"`
}

type Ruleset struct {
	ID           int64  `db:"id"`
	MovetimeMS   int    `db:"movetime_ms"`
	BookPath     string `db:"book_path"`
	BookMaxPlies int    `db:"book_max_plies"`
}

type GameSearchFilter struct {
	EngineID    int64
	WhiteID     int64
	BlackID     int64
	AllowSwap   bool
	MovetimeMS  int
	Result      string
	Termination string
}

// Add a finished game to the database. Returns the inserted games ID.
func (s *Store) InsertFinishedGame(ctx context.Context, whiteID int64, blackID int64, rulesetID int64, result, termination, movesUCI string, bookPlies int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO games (white_player_id, black_player_id, ruleset_id, result, termination, moves_uci, book_plies)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, whiteID, blackID, rulesetID, result, termination, movesUCI, bookPlies)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

// find the most recent game in the database
func (s *Store) LatestGame(ctx context.Context) (GameDetail, error) {
	var gr GameDetail
	if err := s.db.GetContext(ctx, &gr, `
		SELECT g.id,
			g.played_at,
			w.name AS white,
			b.name AS black,
			r.movetime_ms,
			COALESCE(g.result, '*') AS result,
			COALESCE(g.termination, '') AS termination,
			g.moves_uci,
			g.ply_count,
			g.book_plies
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		ORDER BY g.id DESC
		LIMIT 1
	`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GameDetail{}, sql.ErrNoRows
		}
		return GameDetail{}, err
	}
	return gr, nil
}

// list most recent finished games
func (s *Store) ListFinishedGames(ctx context.Context, limit int) ([]GameDetail, error) {
	var out []GameDetail
	err := s.db.SelectContext(ctx, &out, `
		SELECT g.id,
			g.played_at,
			w.name AS white,
			b.name AS black,
			r.movetime_ms,
			COALESCE(g.result, '*') AS result,
			COALESCE(g.termination, '') AS termination,
			g.moves_uci,
			g.ply_count,
			g.book_plies
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		ORDER BY g.id DESC
		LIMIT ?
	`, limit)
	return out, err
}

type GameMovesRow struct {
	MovesUCI string `db:"moves_uci"`
	Result   string `db:"result"`
}

type PairResult struct {
	EngineAID int64
	EngineBID int64
	EngineA   string
	EngineB   string
	WinsA     int
	WinsB     int
	Draws     int
}

type MatchupSummary struct {
	AID        int64
	BID        int64
	A          string
	B          string
	MovetimeMS int
	RulesetID  int64
	WinsA      int
	WinsB      int
	Draws      int
}

type MatchupCount struct {
	WhiteID   int64 `db:"white_id"`
	BlackID   int64 `db:"black_id"`
	RulesetID int64 `db:"ruleset_id"`
	Count     int   `db:"count"`
}

type ResultSummary struct {
	Result      string `db:"result"`
	Termination string `db:"termination"`
	Count       int    `db:"count"`
}

func (s *Store) ListFinishedGamesMoves(ctx context.Context, limit int) ([]GameMovesRow, error) {
	var out []GameMovesRow
	err := s.db.SelectContext(ctx, &out, `
		SELECT moves_uci,
			COALESCE(result, '*') AS result
		FROM games
		ORDER BY id DESC
		LIMIT ?
	`, limit)
	return out, err
}

func (s *Store) ListAllMovesWithResult(ctx context.Context) ([]GameMovesRow, error) {
	var out []GameMovesRow
	err := s.db.SelectContext(ctx, &out, `
		SELECT moves_uci,
			COALESCE(result, '*') AS result
		FROM games
		ORDER BY id ASC
	`)
	return out, err
}

func (s *Store) GameMoves(ctx context.Context, id int64) (moves, result string, err error) {
	var row struct {
		MovesUCI string         `db:"moves_uci"`
		Result   sql.NullString `db:"result"`
	}
	if err := s.db.GetContext(ctx, &row, `SELECT moves_uci, result FROM games WHERE id = ?`, id); err != nil {
		return "", "", err
	}
	if row.Result.Valid {
		result = row.Result.String
	} else {
		result = "*"
	}
	return row.MovesUCI, result, nil
}

func (s *Store) GetGame(ctx context.Context, id int64) (GameDetail, error) {
	var gd GameDetail
	err := s.db.GetContext(ctx, &gd, `
		SELECT g.id,
			g.played_at,
			w.name AS white,
			b.name AS black,
			r.movetime_ms,
			COALESCE(g.result, '*') AS result,
			COALESCE(g.termination, '') AS termination,
			g.moves_uci,
			g.ply_count,
			g.book_plies
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		WHERE g.id = ?
	`, id)
	return gd, err
}

// universal search function
func (s *Store) SearchGames(ctx context.Context, filter GameSearchFilter, limit int) (int, []GameDetail, error) {
	if limit <= 0 {
		limit = 20
	}

	where := "WHERE 1=1"
	args := make([]any, 0, 6)
	if filter.WhiteID != 0 && filter.BlackID != 0 {
		if filter.AllowSwap {
			where += " AND ((white_player_id = ? AND black_player_id = ?) OR (white_player_id = ? AND black_player_id = ?))"
			args = append(args, filter.WhiteID, filter.BlackID, filter.BlackID, filter.WhiteID)
		} else {
			where += " AND white_player_id = ? AND black_player_id = ?"
			args = append(args, filter.WhiteID, filter.BlackID)
		}
	} else if filter.WhiteID != 0 {
		where += " AND white_player_id = ?"
		args = append(args, filter.WhiteID)
	} else if filter.BlackID != 0 {
		where += " AND black_player_id = ?"
		args = append(args, filter.BlackID)
	}
	if filter.EngineID != 0 {
		where += " AND (white_player_id = ? OR black_player_id = ?)"
		args = append(args, filter.EngineID, filter.EngineID)
	}
	if filter.MovetimeMS > 0 {
		where += " AND r.movetime_ms = ?"
		args = append(args, filter.MovetimeMS)
	}
	if filter.Result != "" {
		where += " AND COALESCE(result, '*') = ?"
		args = append(args, filter.Result)
	}
	if filter.Termination != "" {
		where += " AND COALESCE(termination, '') = ?"
		args = append(args, filter.Termination)
	}

	countQuery := "SELECT COUNT(*) FROM games g LEFT JOIN rulesets r ON g.ruleset_id = r.id " + where
	var total int
	if err := s.db.GetContext(ctx, &total, countQuery, args...); err != nil {
		return 0, nil, err
	}

	listQuery := `
		SELECT g.id,
			g.played_at,
			w.name AS white,
			b.name AS black,
			r.movetime_ms,
			COALESCE(g.result, '*') AS result,
			COALESCE(g.termination, '') AS termination,
			g.moves_uci,
			g.ply_count,
			g.book_plies
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		` + where + `
		ORDER BY g.id DESC
		LIMIT ?
	`
	listArgs := append(args, limit)
	var results []GameDetail
	if err := s.db.SelectContext(ctx, &results, listQuery, listArgs...); err != nil {
		return 0, nil, err
	}
	return total, results, nil
}

func (s *Store) ListEngines(ctx context.Context) ([]Engine, error) {
	var out []Engine
	err := s.db.SelectContext(ctx, &out, `
		SELECT id, name,
			COALESCE(engine_path, '') AS engine_path,
			engine_args,
			engine_init,
			COALESCE(engine_elo, 0) AS engine_elo
		FROM players
		WHERE engine_path IS NOT NULL AND engine_path != ''
		ORDER BY id ASC
	`)
	return out, err
}

func (s *Store) ListAllEngines(ctx context.Context) ([]Engine, error) {
	var out []Engine
	err := s.db.SelectContext(ctx, &out, `
		SELECT id, name,
			COALESCE(engine_path, '') AS engine_path,
			engine_args,
			engine_init,
			COALESCE(engine_elo, 0) AS engine_elo
		FROM players
		ORDER BY id ASC
	`)
	return out, err
}

func (s *Store) InsertEngine(ctx context.Context, e Engine) (int64, error) {
	params := engineWriteFrom(e)
	res, err := s.db.NamedExecContext(ctx, `
		INSERT INTO players (name, engine_path, engine_args, engine_init)
		VALUES (:name, :engine_path, :engine_args, :engine_init)
	`, params)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) UpdateEngine(ctx context.Context, e Engine) error {
	params := engineWriteFrom(e)
	params.ID = e.ID
	_, err := s.db.NamedExecContext(ctx, `
		UPDATE players
		SET name = :name,
			engine_path = :engine_path,
			engine_args = :engine_args,
			engine_init = :engine_init
		WHERE id = :id
	`, params)
	return err
}

func (s *Store) DeleteEngine(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM players WHERE id = ?`, id)
	return err
}

func (s *Store) EngineByID(ctx context.Context, id int64) (Engine, error) {
	var e Engine
	err := s.db.GetContext(ctx, &e, `
		SELECT id, name,
			COALESCE(engine_path, '') AS engine_path,
			engine_args,
			engine_init,
			COALESCE(engine_elo, 0) AS engine_elo
		FROM players
		WHERE id = ?
	`, id)
	return e, err
}

func (s *Store) ReplaceEngineElos(ctx context.Context, elos map[int64]float64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `
		UPDATE players
		SET engine_elo = 0
		WHERE engine_path IS NOT NULL AND engine_path != ''
	`); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `UPDATE players SET engine_elo = ? WHERE id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for id, elo := range elos {
		if _, err = stmt.ExecContext(ctx, elo, id); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *Store) EngineIDByName(ctx context.Context, name string) (int64, error) {
	var id int64
	err := s.db.GetContext(ctx, &id, `SELECT id FROM players WHERE name = ?`, name)
	return id, err
}

func (s *Store) EvalByZobrist(ctx context.Context, key uint64) (Eval, error) {
	var e Eval
	err := s.db.GetContext(ctx, &e, `
		SELECT zobrist_key, fen, score, pv, engine_id, depth
		FROM evals
		WHERE zobrist_key = ?
	`, key)
	return e, err
}

func (s *Store) UpsertEval(ctx context.Context, e Eval) error {
	_, err := s.db.NamedExecContext(ctx, `
		INSERT INTO evals (zobrist_key, fen, score, pv, engine_id, depth)
		VALUES (:zobrist_key, :fen, :score, :pv, :engine_id, :depth)
		ON CONFLICT(zobrist_key) DO UPDATE SET
			fen = excluded.fen,
			score = excluded.score,
			pv = excluded.pv,
			engine_id = excluded.engine_id,
			depth = excluded.depth
	`, e)
	return err
}

func (s *Store) InsertRuleset(ctx context.Context, movetimeMS int, bookPath string, bookMaxPlies int) (int64, error) {
	params := rulesetWrite{
		MovetimeMS:   movetimeMS,
		BookPath:     nullableNullString(bookPath),
		BookMaxPlies: bookMaxPlies,
	}
	res, err := s.db.NamedExecContext(ctx, `
		INSERT INTO rulesets (movetime_ms, book_path, book_max_plies)
		VALUES (:movetime_ms, :book_path, :book_max_plies)
	`, params)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

type engineWrite struct {
	ID   int64          `db:"id"`
	Name string         `db:"name"`
	Path sql.NullString `db:"engine_path"`
	Args string         `db:"engine_args"`
	Init string         `db:"engine_init"`
}

func engineWriteFrom(e Engine) engineWrite {
	path := sql.NullString{}
	if strings.TrimSpace(e.Path) != "" {
		path = sql.NullString{String: e.Path, Valid: true}
	}
	return engineWrite{
		Name: e.Name,
		Path: path,
		Args: e.Args,
		Init: e.Init,
	}
}

type rulesetWrite struct {
	MovetimeMS   int            `db:"movetime_ms"`
	BookPath     sql.NullString `db:"book_path"`
	BookMaxPlies int            `db:"book_max_plies"`
}

func nullableNullString(value string) sql.NullString {
	if strings.TrimSpace(value) == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: value, Valid: true}
}

func (s *Store) DeleteRuleset(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM rulesets WHERE id = ?`, id)
	return err
}

func (s *Store) ListRulesets(ctx context.Context) ([]Ruleset, error) {
	var out []Ruleset
	err := s.db.SelectContext(ctx, &out, `
		SELECT id,
			movetime_ms,
			COALESCE(book_path, '') AS book_path,
			book_max_plies
		FROM rulesets
		ORDER BY id ASC
	`)
	return out, err
}

func (s *Store) RulesetByID(ctx context.Context, id int64) (Ruleset, error) {
	var r Ruleset
	err := s.db.GetContext(ctx, &r, `
		SELECT id,
			movetime_ms,
			COALESCE(book_path, '') AS book_path,
			book_max_plies
		FROM rulesets
		WHERE id = ?
	`, id)
	return r, err
}

func (s *Store) ListMatchups(ctx context.Context) ([]Matchup, error) {
	var out []Matchup
	err := s.db.SelectContext(ctx, &out, `
		SELECT id, player_a_id, player_b_id, ruleset_id
		FROM matchups
		ORDER BY id ASC
	`)
	return out, err
}

func (s *Store) ReplaceMatchups(ctx context.Context, matchups []Matchup) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM matchups`); err != nil {
		return err
	}
	for _, m := range matchups {
		_, err := s.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO matchups (player_a_id, player_b_id, ruleset_id)
			VALUES (?, ?, ?)
		`, m.PlayerAID, m.PlayerBID, m.RulesetID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ReplaceMatchupsForRuleset(ctx context.Context, rulesetID int64, matchups []Matchup) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM matchups WHERE ruleset_id = ?`, rulesetID); err != nil {
		return err
	}
	for _, m := range matchups {
		_, err := s.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO matchups (player_a_id, player_b_id, ruleset_id)
			VALUES (?, ?, ?)
		`, m.PlayerAID, m.PlayerBID, m.RulesetID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DeleteMatchupsByEngine(ctx context.Context, engineID int64) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM matchups
		WHERE player_a_id = ? OR player_b_id = ?
	`, engineID, engineID)
	if err != nil {
		return 0, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func (s *Store) EngineGameCounts(ctx context.Context) (map[int64]int, error) {
	type countRow struct {
		ID    int64 `db:"id"`
		Count int   `db:"count"`
	}
	var rows []countRow
	if err := s.db.SelectContext(ctx, &rows, `
		SELECT white_player_id AS id, COUNT(*) AS count FROM games GROUP BY white_player_id
		UNION ALL
		SELECT black_player_id AS id, COUNT(*) AS count FROM games GROUP BY black_player_id
	`); err != nil {
		return nil, err
	}
	counts := make(map[int64]int)
	for _, row := range rows {
		counts[row.ID] += row.Count
	}
	return counts, nil
}

func (s *Store) EngineMatchupCounts(ctx context.Context) (map[int64]int, error) {
	type countRow struct {
		ID    int64 `db:"id"`
		Count int   `db:"count"`
	}
	var rows []countRow
	if err := s.db.SelectContext(ctx, &rows, `
		SELECT player_a_id AS id, COUNT(*) AS count FROM matchups GROUP BY player_a_id
		UNION ALL
		SELECT player_b_id AS id, COUNT(*) AS count FROM matchups WHERE player_b_id != player_a_id GROUP BY player_b_id
	`); err != nil {
		return nil, err
	}
	counts := make(map[int64]int)
	for _, row := range rows {
		counts[row.ID] += row.Count
	}
	return counts, nil
}

func (s *Store) DeleteGamesByEngine(ctx context.Context, engineID int64) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM games
		WHERE white_player_id = ? OR black_player_id = ?
	`, engineID, engineID)
	if err != nil {
		return 0, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func (s *Store) ListResults(ctx context.Context) ([]string, error) {
	var raw []string
	err := s.db.SelectContext(ctx, &raw, `
		SELECT DISTINCT COALESCE(result, '*') AS result
		FROM games
		ORDER BY 1
	`)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, res := range raw {
		if res != "" {
			out = append(out, res)
		}
	}
	return out, nil
}

func (s *Store) ListTerminations(ctx context.Context) ([]string, error) {
	var raw []string
	err := s.db.SelectContext(ctx, &raw, `
		SELECT DISTINCT COALESCE(termination, '') AS termination
		FROM games
		WHERE termination IS NOT NULL
		ORDER BY 1
	`)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, term := range raw {
		if term != "" {
			out = append(out, term)
		}
	}
	return out, nil
}

func (s *Store) ResultsByPair(ctx context.Context) ([]PairResult, error) {
	type pairRow struct {
		WhiteID int64  `db:"white_player_id"`
		BlackID int64  `db:"black_player_id"`
		White   string `db:"white"`
		Black   string `db:"black"`
		Result  string `db:"result"`
		Count   int    `db:"count"`
	}
	var rows []pairRow
	if err := s.db.SelectContext(ctx, &rows, `
		SELECT g.white_player_id,
			g.black_player_id,
			w.name AS white,
			b.name AS black,
			COALESCE(g.result, '*') AS result,
			COUNT(*) AS count
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		GROUP BY g.white_player_id, g.black_player_id, result
	`); err != nil {
		return nil, err
	}

	counts := make(map[[2]int64]*PairResult)
	for _, row := range rows {
		whiteID := row.WhiteID
		blackID := row.BlackID
		white := row.White
		black := row.Black
		result := row.Result
		count := row.Count
		if result != "1-0" && result != "0-1" && result != "1/2-1/2" {
			continue
		}
		a, b := white, black
		aID, bID := whiteID, blackID
		swap := false
		if aID > bID {
			a, b = b, a
			aID, bID = bID, aID
			swap = true
		}
		key := [2]int64{aID, bID}
		entry, ok := counts[key]
		if !ok {
			entry = &PairResult{EngineA: a, EngineB: b, EngineAID: aID, EngineBID: bID}
			counts[key] = entry
		}
		switch result {
		case "1-0":
			if swap {
				entry.WinsB += count
			} else {
				entry.WinsA += count
			}
		case "0-1":
			if swap {
				entry.WinsA += count
			} else {
				entry.WinsB += count
			}
		case "1/2-1/2":
			entry.Draws += count
		}
	}

	results := make([]PairResult, 0, len(counts))
	for _, entry := range counts {
		results = append(results, *entry)
	}
	return results, nil
}

func (s *Store) ListMatchupSummaries(ctx context.Context) ([]MatchupSummary, error) {
	type summaryRow struct {
		WhiteID   int64  `db:"white_player_id"`
		BlackID   int64  `db:"black_player_id"`
		White     string `db:"white"`
		Black     string `db:"black"`
		Movetime  int    `db:"movetime_ms"`
		RulesetID int64  `db:"ruleset_id"`
		Result    string `db:"result"`
		Count     int    `db:"count"`
	}
	var rows []summaryRow
	if err := s.db.SelectContext(ctx, &rows, `
		SELECT g.white_player_id,
			g.black_player_id,
			w.name AS white,
			b.name AS black,
			r.movetime_ms,
			g.ruleset_id,
			COALESCE(g.result, '*') AS result,
			COUNT(*) AS count
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		GROUP BY g.white_player_id, g.black_player_id, g.ruleset_id, r.movetime_ms, result
	`); err != nil {
		return nil, err
	}

	counts := make(map[[3]int64]*MatchupSummary)
	for _, row := range rows {
		whiteID := row.WhiteID
		blackID := row.BlackID
		white := row.White
		black := row.Black
		movetime := row.Movetime
		rulesetID := row.RulesetID
		result := row.Result
		count := row.Count
		if result != "1-0" && result != "0-1" && result != "1/2-1/2" {
			continue
		}
		a, b := white, black
		aID, bID := whiteID, blackID
		swap := false
		if aID > bID {
			a, b = b, a
			aID, bID = bID, aID
			swap = true
		}
		key := [3]int64{aID, bID, rulesetID}
		entry, ok := counts[key]
		if !ok {
			entry = &MatchupSummary{A: a, B: b, AID: aID, BID: bID, MovetimeMS: movetime, RulesetID: rulesetID}
			counts[key] = entry
		}
		switch result {
		case "1-0":
			if swap {
				entry.WinsB += count
			} else {
				entry.WinsA += count
			}
		case "0-1":
			if swap {
				entry.WinsA += count
			} else {
				entry.WinsB += count
			}
		case "1/2-1/2":
			entry.Draws += count
		}
	}

	results := make([]MatchupSummary, 0, len(counts))
	for _, entry := range counts {
		results = append(results, *entry)
	}
	return results, nil
}

func (s *Store) ListMatchupCounts(ctx context.Context) ([]MatchupCount, error) {
	var out []MatchupCount
	err := s.db.SelectContext(ctx, &out, `
		SELECT g.white_player_id AS white_id,
			g.black_player_id AS black_id,
			g.ruleset_id,
			COUNT(*) AS count
		FROM games g
		GROUP BY g.white_player_id, g.black_player_id, g.ruleset_id
	`)
	return out, err
}

func (s *Store) ListResultSummaries(ctx context.Context) ([]ResultSummary, error) {
	var out []ResultSummary
	err := s.db.SelectContext(ctx, &out, `
		SELECT COALESCE(result, '*') AS result,
			COALESCE(termination, '') AS termination,
			COUNT(*) AS count
		FROM games
		GROUP BY result, termination
	`)
	return out, err
}

// MatchupMovesLines returns one line per game for a specific matchup and movetime.
func (s *Store) MatchupMovesLines(ctx context.Context, a, b int64, movetimeMS int) (string, error) {
	var rows []GameMovesRow
	if err := s.db.SelectContext(ctx, &rows, `
		SELECT moves_uci,
			COALESCE(result, '*') AS result
		FROM games
		WHERE ruleset_id IN (SELECT id FROM rulesets WHERE movetime_ms = ?)
		  AND ((white_player_id = ? AND black_player_id = ?) OR (white_player_id = ? AND black_player_id = ?))
		ORDER BY id ASC
	`, movetimeMS, a, b, b, a); err != nil {
		return "", err
	}

	out := ""
	for _, row := range rows {
		result := row.Result
		if result == "" {
			result = "*"
		}
		if row.MovesUCI != "" {
			out += row.MovesUCI + " " + result + "\n"
		} else {
			out += result + "\n"
		}
	}
	return out, nil
}

// ResultMovesLines returns one line per game for a specific result/termination.
func (s *Store) ResultMovesLines(ctx context.Context, result, termination string) (string, error) {
	var rows []GameMovesRow
	if err := s.db.SelectContext(ctx, &rows, `
		SELECT moves_uci,
			COALESCE(result, '*') AS result
		FROM games
		WHERE COALESCE(result, '*') = ? AND COALESCE(termination, '') = ?
		ORDER BY id ASC
	`, result, termination); err != nil {
		return "", err
	}

	out := ""
	for _, row := range rows {
		lineResult := row.Result
		if lineResult == "" {
			lineResult = "*"
		}
		if row.MovesUCI != "" {
			out += row.MovesUCI + " " + lineResult + "\n"
		} else {
			out += lineResult + "\n"
		}
	}
	return out, nil
}

func (s *Store) DeleteMatchupGames(ctx context.Context, a, b int64, movetimeMS int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM games
		WHERE ruleset_id IN (SELECT id FROM rulesets WHERE movetime_ms = ?)
		  AND ((white_player_id = ? AND black_player_id = ?) OR (white_player_id = ? AND black_player_id = ?))
	`, movetimeMS, a, b, b, a)
	if err != nil {
		return 0, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func (s *Store) DeleteResultGames(ctx context.Context, result, termination string) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM games
		WHERE COALESCE(result, '*') = ? AND COALESCE(termination, '') = ?
	`, result, termination)
	if err != nil {
		return 0, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// AllFinishedMovesLines returns one line per game: "<moves> <result>".
func (s *Store) AllFinishedMovesLines(ctx context.Context) (string, error) {
	var rows []GameMovesRow
	if err := s.db.SelectContext(ctx, &rows, `
		SELECT moves_uci,
			COALESCE(result, '*') AS result
		FROM games
		ORDER BY id ASC
	`); err != nil {
		return "", err
	}

	out := ""
	for _, row := range rows {
		result := row.Result
		if result == "" {
			result = "*"
		}
		if row.MovesUCI != "" {
			out += row.MovesUCI + " " + result + "\n"
		} else {
			out += result + "\n"
		}
	}
	return out, nil
}

func (s *Store) CountGames(ctx context.Context) (int, error) {
	var count int
	err := s.db.GetContext(ctx, &count, `SELECT COUNT(*) FROM games`)
	return count, err
}

func (s *Store) CountEngines(ctx context.Context) (int, error) {
	var count int
	err := s.db.GetContext(ctx, &count, `
		SELECT COUNT(*)
		FROM players
		WHERE engine_path IS NOT NULL AND engine_path != ''
	`)
	return count, err
}
