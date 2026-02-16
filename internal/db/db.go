package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var schema_stmts = []string{
	`PRAGMA journal_mode=WAL;`,
	`PRAGMA foreign_keys=ON;`,
	`CREATE TABLE IF NOT EXISTS players (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		engine_path TEXT NULL,
		engine_args TEXT NOT NULL DEFAULT '',
		engine_init TEXT NOT NULL DEFAULT '',
		engine_elo REAL NOT NULL DEFAULT 0,
		UNIQUE(name)
	);`,
	`CREATE TABLE IF NOT EXISTS rulesets (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		movetime_ms INTEGER NOT NULL DEFAULT 100,
		book_path TEXT NULL DEFAULT NULL,
		book_max_plies INTEGER NOT NULL DEFAULT 0,
		UNIQUE(movetime_ms, book_path, book_max_plies)
	);`,
	`CREATE TABLE IF NOT EXISTS matchups (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		player_a_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		player_b_id INTEGER NOT NULL REFERENCES players(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		ruleset_id INTEGER NOT NULL REFERENCES rulesets(id) ON UPDATE CASCADE ON DELETE RESTRICT,
		UNIQUE(player_a_id, player_b_id, ruleset_id)
	);`,
	`CREATE TABLE IF NOT EXISTS games (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
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
	`CREATE INDEX IF NOT EXISTS idx_games_played_at ON games(played_at);`,
	`CREATE INDEX IF NOT EXISTS idx_games_white_player_id ON games(white_player_id);`,
	`CREATE INDEX IF NOT EXISTS idx_games_black_player_id ON games(black_player_id);`,
	`CREATE INDEX IF NOT EXISTS idx_games_matchup ON games(white_player_id, black_player_id, ruleset_id);`,
	`CREATE INDEX IF NOT EXISTS idx_evals_engine_id ON evals(engine_id);`,
	`CREATE INDEX IF NOT EXISTS idx_matchups_player_a_id ON matchups(player_a_id);`,
	`CREATE INDEX IF NOT EXISTS idx_matchups_player_b_id ON matchups(player_b_id);`,
	`CREATE INDEX IF NOT EXISTS idx_matchups_ruleset_id ON matchups(ruleset_id);`,
}

const defaultRulesetMovetimeMS = 100

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
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
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}
	if err := ensureDefaultRulesetExists(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("default ruleset: %w", err)
	}
	if err := ensureEngineEloColumn(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("engine elo: %w", err)
	}
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

func ensureDefaultRulesetExists(db *sql.DB) error {
	row := db.QueryRow(`SELECT id FROM rulesets ORDER BY id ASC LIMIT 1`)
	var id int64
	if err := row.Scan(&id); err == nil {
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err := db.Exec(`
		INSERT INTO rulesets (movetime_ms, book_path, book_max_plies)
		VALUES (?, NULL, 0)
	`, defaultRulesetMovetimeMS)
	return err
}

func ensureEngineEloColumn(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(players)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == "engine_elo" {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.Exec(`ALTER TABLE players ADD COLUMN engine_elo REAL NOT NULL DEFAULT 0`)
	return err
}

type GameDetail struct {
	ID          int64
	PlayedAt    string
	White       string
	Black       string
	MovetimeMS  int
	Result      string
	Termination string
	MovesUCI    string
	Plies       int
	BookPlies   int
}

type Eval struct {
	ZobristKey uint64
	FEN        string
	Score      string
	PV         string
	EngineID   int64
	Depth      int
}

type Engine struct {
	ID   int64
	Name string
	Path string
	Args string
	Init string
	Elo  float64
}

type Matchup struct {
	ID        int64
	PlayerAID int64
	PlayerBID int64
	RulesetID int64
}

type Ruleset struct {
	ID           int64
	MovetimeMS   int
	BookPath     string
	BookMaxPlies int
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
	row := s.db.QueryRowContext(ctx, `
		SELECT g.id, g.played_at, w.name, b.name, r.movetime_ms, g.result, g.termination, g.moves_uci, g.ply_count, g.book_plies
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		ORDER BY g.id DESC
		LIMIT 1
	`)
	var gr GameDetail
	if err := row.Scan(
		&gr.ID, &gr.PlayedAt,
		&gr.White, &gr.Black, &gr.MovetimeMS,
		&gr.Result, &gr.Termination, &gr.MovesUCI, &gr.Plies, &gr.BookPlies,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GameDetail{}, sql.ErrNoRows
		}
		return GameDetail{}, err
	}
	return gr, nil
}

// list most recent finished games
func (s *Store) ListFinishedGames(ctx context.Context, limit int) ([]GameDetail, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.id, g.played_at, w.name, b.name, r.movetime_ms, g.result, g.termination, g.moves_uci, g.ply_count, g.book_plies
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		ORDER BY g.id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []GameDetail
	for rows.Next() {
		var gr GameDetail
		if err := rows.Scan(
			&gr.ID, &gr.PlayedAt,
			&gr.White, &gr.Black, &gr.MovetimeMS,
			&gr.Result, &gr.Termination, &gr.MovesUCI, &gr.Plies, &gr.BookPlies,
		); err != nil {
			return nil, err
		}
		out = append(out, gr)
	}
	return out, rows.Err()
}

type GameMovesRow struct {
	MovesUCI string
	Result   string
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
	WhiteID   int64
	BlackID   int64
	RulesetID int64
	Count     int
}

type ResultSummary struct {
	Result      string
	Termination string
	Count       int
}

func (s *Store) ListFinishedGamesMoves(ctx context.Context, limit int) ([]GameMovesRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT moves_uci, COALESCE(result, '*')
		FROM games
		ORDER BY id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []GameMovesRow
	for rows.Next() {
		var row GameMovesRow
		if err := rows.Scan(&row.MovesUCI, &row.Result); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) ListAllMovesWithResult(ctx context.Context) ([]GameMovesRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT moves_uci, COALESCE(result, '*')
		FROM games
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []GameMovesRow
	for rows.Next() {
		var row GameMovesRow
		if err := rows.Scan(&row.MovesUCI, &row.Result); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) GameMoves(ctx context.Context, id int64) (moves, result string, err error) {
	row := s.db.QueryRowContext(ctx, `SELECT moves_uci, result FROM games WHERE id = ?`, id)
	var res sql.NullString
	if err := row.Scan(&moves, &res); err != nil {
		return "", "", err
	}
	if res.Valid {
		result = res.String
	} else {
		result = "*"
	}
	return moves, result, nil
}

func (s *Store) GetGame(ctx context.Context, id int64) (GameDetail, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT g.id, g.played_at, w.name, b.name, r.movetime_ms, COALESCE(g.result, '*'), COALESCE(g.termination, ''), g.moves_uci, g.ply_count, g.book_plies
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		WHERE g.id = ?
	`, id)
	var gd GameDetail
	if err := row.Scan(
		&gd.ID, &gd.PlayedAt,
		&gd.White, &gd.Black, &gd.MovetimeMS,
		&gd.Result, &gd.Termination, &gd.MovesUCI, &gd.Plies, &gd.BookPlies,
	); err != nil {
		return GameDetail{}, err
	}
	return gd, nil
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
	row := s.db.QueryRowContext(ctx, countQuery, args...)
	var total int
	if err := row.Scan(&total); err != nil {
		return 0, nil, err
	}

	listQuery := `
		SELECT g.id, g.played_at, w.name, b.name, r.movetime_ms, COALESCE(g.result, '*'), COALESCE(g.termination, ''), g.moves_uci, g.ply_count, g.book_plies
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		` + where + `
		ORDER BY g.id DESC
		LIMIT ?
	`
	listArgs := append(args, limit)
	rows, err := s.db.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	results := make([]GameDetail, 0)
	for rows.Next() {
		var gd GameDetail
		if err := rows.Scan(
			&gd.ID, &gd.PlayedAt,
			&gd.White, &gd.Black, &gd.MovetimeMS,
			&gd.Result, &gd.Termination, &gd.MovesUCI, &gd.Plies, &gd.BookPlies,
		); err != nil {
			return 0, nil, err
		}
		results = append(results, gd)
	}
	return total, results, rows.Err()
}

func (s *Store) ListEngines(ctx context.Context) ([]Engine, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, COALESCE(engine_path, ''), engine_args, engine_init, COALESCE(engine_elo, 0)
		FROM players
		WHERE engine_path IS NOT NULL AND engine_path != ''
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Engine
	for rows.Next() {
		var e Engine
		if err := rows.Scan(&e.ID, &e.Name, &e.Path, &e.Args, &e.Init, &e.Elo); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) InsertEngine(ctx context.Context, e Engine) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO players (name, engine_path, engine_args, engine_init)
		VALUES (?, ?, ?, ?)
	`, e.Name, nullableString(e.Path), e.Args, e.Init)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) UpdateEngine(ctx context.Context, e Engine) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE players
		SET name = ?, engine_path = ?, engine_args = ?, engine_init = ?
		WHERE id = ?
	`, e.Name, nullableString(e.Path), e.Args, e.Init, e.ID)
	return err
}

func (s *Store) DeleteEngine(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM players WHERE id = ?`, id)
	return err
}

func (s *Store) EngineByID(ctx context.Context, id int64) (Engine, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, COALESCE(engine_path, ''), engine_args, engine_init, COALESCE(engine_elo, 0)
		FROM players
		WHERE id = ?
	`, id)
	var e Engine
	if err := row.Scan(&e.ID, &e.Name, &e.Path, &e.Args, &e.Init, &e.Elo); err != nil {
		return Engine{}, err
	}
	return e, nil
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
	row := s.db.QueryRowContext(ctx, `SELECT id FROM players WHERE name = ?`, name)
	var id int64
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) EvalByZobrist(ctx context.Context, key uint64) (Eval, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT zobrist_key, fen, score, pv, engine_id, depth
		FROM evals
		WHERE zobrist_key = ?
	`, key)
	var e Eval
	if err := row.Scan(&e.ZobristKey, &e.FEN, &e.Score, &e.PV, &e.EngineID, &e.Depth); err != nil {
		return Eval{}, err
	}
	return e, nil
}

func (s *Store) UpsertEval(ctx context.Context, e Eval) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO evals (zobrist_key, fen, score, pv, engine_id, depth)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(zobrist_key) DO UPDATE SET
			fen = excluded.fen,
			score = excluded.score,
			pv = excluded.pv,
			engine_id = excluded.engine_id,
			depth = excluded.depth
	`, e.ZobristKey, e.FEN, e.Score, e.PV, e.EngineID, e.Depth)
	return err
}

func (s *Store) InsertRuleset(ctx context.Context, movetimeMS int, bookPath string, bookMaxPlies int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO rulesets (movetime_ms, book_path, book_max_plies)
		VALUES (?, ?, ?)
	`, movetimeMS, nullableString(bookPath), bookMaxPlies)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) DeleteRuleset(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM rulesets WHERE id = ?`, id)
	return err
}

func (s *Store) ListRulesets(ctx context.Context) ([]Ruleset, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, movetime_ms, COALESCE(book_path, ''), book_max_plies
		FROM rulesets
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Ruleset
	for rows.Next() {
		var r Ruleset
		if err := rows.Scan(&r.ID, &r.MovetimeMS, &r.BookPath, &r.BookMaxPlies); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) RulesetByID(ctx context.Context, id int64) (Ruleset, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, movetime_ms, COALESCE(book_path, ''), book_max_plies
		FROM rulesets
		WHERE id = ?
	`, id)
	var r Ruleset
	if err := row.Scan(&r.ID, &r.MovetimeMS, &r.BookPath, &r.BookMaxPlies); err != nil {
		return Ruleset{}, err
	}
	return r, nil
}

func (s *Store) ListMatchups(ctx context.Context) ([]Matchup, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, player_a_id, player_b_id, ruleset_id
		FROM matchups
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Matchup
	for rows.Next() {
		var m Matchup
		if err := rows.Scan(&m.ID, &m.PlayerAID, &m.PlayerBID, &m.RulesetID); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
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
	rows, err := s.db.QueryContext(ctx, `
		SELECT white_player_id, COUNT(*) FROM games GROUP BY white_player_id
		UNION ALL
		SELECT black_player_id, COUNT(*) FROM games GROUP BY black_player_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[int64]int)
	for rows.Next() {
		var id int64
		var count int
		if err := rows.Scan(&id, &count); err != nil {
			return nil, err
		}
		counts[id] += count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return counts, nil
}

func (s *Store) EngineMatchupCounts(ctx context.Context) (map[int64]int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT player_a_id, COUNT(*) FROM matchups GROUP BY player_a_id
		UNION ALL
		SELECT player_b_id, COUNT(*) FROM matchups WHERE player_b_id != player_a_id GROUP BY player_b_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[int64]int)
	for rows.Next() {
		var id int64
		var count int
		if err := rows.Scan(&id, &count); err != nil {
			return nil, err
		}
		counts[id] += count
	}
	if err := rows.Err(); err != nil {
		return nil, err
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
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT COALESCE(result, '*')
		FROM games
		ORDER BY 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var res string
		if err := rows.Scan(&res); err != nil {
			return nil, err
		}
		if res != "" {
			out = append(out, res)
		}
	}
	return out, rows.Err()
}

func (s *Store) ListTerminations(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT COALESCE(termination, '')
		FROM games
		WHERE termination IS NOT NULL
		ORDER BY 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var term string
		if err := rows.Scan(&term); err != nil {
			return nil, err
		}
		if term != "" {
			out = append(out, term)
		}
	}
	return out, rows.Err()
}

func (s *Store) ResultsByPair(ctx context.Context) ([]PairResult, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.white_player_id, g.black_player_id, w.name, b.name, COALESCE(g.result, '*') as result,
		       COUNT(*)
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		GROUP BY g.white_player_id, g.black_player_id, result
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[[2]int64]*PairResult)
	for rows.Next() {
		var white, black, result string
		var whiteID, blackID int64
		var count int
		if err := rows.Scan(&whiteID, &blackID, &white, &black, &result, &count); err != nil {
			return nil, err
		}
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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]PairResult, 0, len(counts))
	for _, entry := range counts {
		results = append(results, *entry)
	}
	return results, nil
}

func (s *Store) ListMatchupSummaries(ctx context.Context) ([]MatchupSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.white_player_id, g.black_player_id, w.name, b.name, r.movetime_ms, g.ruleset_id, COALESCE(g.result, '*') as result, COUNT(*)
		FROM games g
		LEFT JOIN players w ON g.white_player_id = w.id
		LEFT JOIN players b ON g.black_player_id = b.id
		LEFT JOIN rulesets r ON g.ruleset_id = r.id
		GROUP BY g.white_player_id, g.black_player_id, g.ruleset_id, r.movetime_ms, result
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[[3]int64]*MatchupSummary)
	for rows.Next() {
		var white, black, result string
		var whiteID, blackID int64
		var movetime int
		var rulesetID int64
		var count int
		if err := rows.Scan(&whiteID, &blackID, &white, &black, &movetime, &rulesetID, &result, &count); err != nil {
			return nil, err
		}
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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]MatchupSummary, 0, len(counts))
	for _, entry := range counts {
		results = append(results, *entry)
	}
	return results, nil
}

func (s *Store) ListMatchupCounts(ctx context.Context) ([]MatchupCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.white_player_id, g.black_player_id, g.ruleset_id, COUNT(*)
		FROM games g
		GROUP BY g.white_player_id, g.black_player_id, g.ruleset_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MatchupCount
	for rows.Next() {
		var row MatchupCount
		if err := rows.Scan(&row.WhiteID, &row.BlackID, &row.RulesetID, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) ListResultSummaries(ctx context.Context) ([]ResultSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT COALESCE(result, '*') as result, COALESCE(termination, '') as termination, COUNT(*)
		FROM games
		GROUP BY result, termination
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ResultSummary
	for rows.Next() {
		var row ResultSummary
		if err := rows.Scan(&row.Result, &row.Termination, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// MatchupMovesLines returns one line per game for a specific matchup and movetime.
func (s *Store) MatchupMovesLines(ctx context.Context, a, b int64, movetimeMS int) (string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT moves_uci, result
		FROM games
		WHERE ruleset_id IN (SELECT id FROM rulesets WHERE movetime_ms = ?)
		  AND ((white_player_id = ? AND black_player_id = ?) OR (white_player_id = ? AND black_player_id = ?))
		ORDER BY id ASC
	`, movetimeMS, a, b, b, a)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	out := ""
	for rows.Next() {
		var moves string
		var res sql.NullString
		if err := rows.Scan(&moves, &res); err != nil {
			return "", err
		}
		result := "*"
		if res.Valid && res.String != "" {
			result = res.String
		}
		if moves != "" {
			out += moves + " " + result + "\n"
		} else {
			out += result + "\n"
		}
	}
	return out, rows.Err()
}

// ResultMovesLines returns one line per game for a specific result/termination.
func (s *Store) ResultMovesLines(ctx context.Context, result, termination string) (string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT moves_uci, result
		FROM games
		WHERE COALESCE(result, '*') = ? AND COALESCE(termination, '') = ?
		ORDER BY id ASC
	`, result, termination)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	out := ""
	for rows.Next() {
		var moves string
		var res sql.NullString
		if err := rows.Scan(&moves, &res); err != nil {
			return "", err
		}
		lineResult := "*"
		if res.Valid && res.String != "" {
			lineResult = res.String
		}
		if moves != "" {
			out += moves + " " + lineResult + "\n"
		} else {
			out += lineResult + "\n"
		}
	}
	return out, rows.Err()
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
	rows, err := s.db.QueryContext(ctx, `
		SELECT moves_uci, result
		FROM games
		ORDER BY id ASC
	`)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	out := ""
	for rows.Next() {
		var moves string
		var res sql.NullString
		if err := rows.Scan(&moves, &res); err != nil {
			return "", err
		}
		result := "*"
		if res.Valid && res.String != "" {
			result = res.String
		}
		if moves != "" {
			out += moves + " " + result + "\n"
		} else {
			out += result + "\n"
		}
	}
	return out, rows.Err()
}

func (s *Store) CountGames(ctx context.Context) (int, error) {
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM games`)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) CountEngines(ctx context.Context) (int, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM players
		WHERE engine_path IS NOT NULL AND engine_path != ''
	`)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
