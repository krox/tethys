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

func Open(path string) (*sql.DB, error) {
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

	return db, nil
}

func Migrate(db *sql.DB) error {
	stmts := []string{
		`PRAGMA journal_mode=WAL;`,
		`PRAGMA foreign_keys=ON;`,
		`CREATE TABLE IF NOT EXISTS engines (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			path TEXT NOT NULL DEFAULT '',
			args TEXT NOT NULL DEFAULT '',
			init TEXT NOT NULL DEFAULT ''
		);`,
		`CREATE TABLE IF NOT EXISTS games (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			played_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			engine_white_id INTEGER NOT NULL REFERENCES engines(id) ON UPDATE CASCADE ON DELETE RESTRICT,
			engine_black_id INTEGER NOT NULL REFERENCES engines(id) ON UPDATE CASCADE ON DELETE RESTRICT,
			movetime_ms INTEGER NOT NULL,
			result TEXT,         -- 1-0|0-1|1/2-1/2|*
			termination TEXT,
			moves_uci TEXT NOT NULL DEFAULT '',
			book_plies INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS matchups (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			engine_a_id INTEGER NOT NULL REFERENCES engines(id) ON UPDATE CASCADE ON DELETE RESTRICT,
			engine_b_id INTEGER NOT NULL REFERENCES engines(id) ON UPDATE CASCADE ON DELETE RESTRICT,
			UNIQUE(engine_a_id, engine_b_id)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_games_played_at ON games(played_at);`,
		`CREATE INDEX IF NOT EXISTS idx_games_engine_white_id ON games(engine_white_id);`,
		`CREATE INDEX IF NOT EXISTS idx_games_engine_black_id ON games(engine_black_id);`,
		`CREATE INDEX IF NOT EXISTS idx_games_matchup ON games(engine_white_id, engine_black_id, movetime_ms);`,
		`CREATE INDEX IF NOT EXISTS idx_matchups_engine_a_id ON matchups(engine_a_id);`,
		`CREATE INDEX IF NOT EXISTS idx_matchups_engine_b_id ON matchups(engine_b_id);`,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}

	return nil
}

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

type GameRow struct {
	ID          int64
	PlayedAt    string
	White       string
	Black       string
	MovetimeMS  int
	Result      sql.NullString
	Termination sql.NullString
	MovesUCI    string
	BookPlies   int
}

type Engine struct {
	ID   int64
	Name string
	Path string
	Args string
	Init string
}

type Matchup struct {
	ID        int64
	EngineAID int64
	EngineBID int64
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
	BookPlies   int
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

func (s *Store) InsertFinishedGame(ctx context.Context, whiteID int64, blackID int64, movetimeMS int, result, termination, movesUCI string, bookPlies int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO games (engine_white_id, engine_black_id, movetime_ms, result, termination, moves_uci, book_plies)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, whiteID, blackID, movetimeMS, result, termination, movesUCI, bookPlies)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) LatestGame(ctx context.Context) (GameRow, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT g.id, g.played_at,
		       COALESCE(w.name, ''),
		       COALESCE(b.name, ''),
		       g.movetime_ms, g.result, g.termination, g.moves_uci, g.book_plies
		FROM games g
		LEFT JOIN engines w ON g.engine_white_id = w.id
		LEFT JOIN engines b ON g.engine_black_id = b.id
		ORDER BY g.id DESC
		LIMIT 1
	`)
	var gr GameRow
	if err := row.Scan(
		&gr.ID, &gr.PlayedAt,
		&gr.White, &gr.Black, &gr.MovetimeMS,
		&gr.Result, &gr.Termination, &gr.MovesUCI, &gr.BookPlies,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GameRow{}, sql.ErrNoRows
		}
		return GameRow{}, err
	}
	return gr, nil
}

func (s *Store) ListFinishedGames(ctx context.Context, limit int) ([]GameRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.id, g.played_at,
		       COALESCE(w.name, ''),
		       COALESCE(b.name, ''),
		       g.movetime_ms, g.result, g.termination, g.moves_uci, g.book_plies
		FROM games g
		LEFT JOIN engines w ON g.engine_white_id = w.id
		LEFT JOIN engines b ON g.engine_black_id = b.id
		ORDER BY g.id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []GameRow
	for rows.Next() {
		var gr GameRow
		if err := rows.Scan(
			&gr.ID, &gr.PlayedAt,
			&gr.White, &gr.Black, &gr.MovetimeMS,
			&gr.Result, &gr.Termination, &gr.MovesUCI, &gr.BookPlies,
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
	WinsA      int
	WinsB      int
	Draws      int
}

type MatchupCount struct {
	WhiteID    int64
	BlackID    int64
	MovetimeMS int
	Count      int
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
		SELECT g.id, g.played_at,
		       COALESCE(w.name, ''),
		       COALESCE(b.name, ''),
		       g.movetime_ms, COALESCE(g.result, '*'), COALESCE(g.termination, ''), g.moves_uci, g.book_plies
		FROM games g
		LEFT JOIN engines w ON g.engine_white_id = w.id
		LEFT JOIN engines b ON g.engine_black_id = b.id
		WHERE g.id = ?
	`, id)
	var gd GameDetail
	if err := row.Scan(
		&gd.ID, &gd.PlayedAt,
		&gd.White, &gd.Black, &gd.MovetimeMS,
		&gd.Result, &gd.Termination, &gd.MovesUCI, &gd.BookPlies,
	); err != nil {
		return GameDetail{}, err
	}
	return gd, nil
}

func (s *Store) SearchGames(ctx context.Context, filter GameSearchFilter, limit int) (int, []GameDetail, error) {
	if limit <= 0 {
		limit = 20
	}

	where := "WHERE 1=1"
	args := make([]any, 0, 6)
	if filter.WhiteID != 0 && filter.BlackID != 0 {
		if filter.AllowSwap {
			where += " AND ((engine_white_id = ? AND engine_black_id = ?) OR (engine_white_id = ? AND engine_black_id = ?))"
			args = append(args, filter.WhiteID, filter.BlackID, filter.BlackID, filter.WhiteID)
		} else {
			where += " AND engine_white_id = ? AND engine_black_id = ?"
			args = append(args, filter.WhiteID, filter.BlackID)
		}
	} else if filter.WhiteID != 0 {
		where += " AND engine_white_id = ?"
		args = append(args, filter.WhiteID)
	} else if filter.BlackID != 0 {
		where += " AND engine_black_id = ?"
		args = append(args, filter.BlackID)
	}
	if filter.EngineID != 0 {
		where += " AND (engine_white_id = ? OR engine_black_id = ?)"
		args = append(args, filter.EngineID, filter.EngineID)
	}
	if filter.MovetimeMS > 0 {
		where += " AND movetime_ms = ?"
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

	countQuery := "SELECT COUNT(*) FROM games " + where
	row := s.db.QueryRowContext(ctx, countQuery, args...)
	var total int
	if err := row.Scan(&total); err != nil {
		return 0, nil, err
	}

	listQuery := `
		SELECT g.id, g.played_at,
		       COALESCE(w.name, ''),
		       COALESCE(b.name, ''),
		       g.movetime_ms, COALESCE(g.result, '*'), COALESCE(g.termination, ''), g.moves_uci, g.book_plies
		FROM games g
		LEFT JOIN engines w ON g.engine_white_id = w.id
		LEFT JOIN engines b ON g.engine_black_id = b.id
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
			&gd.Result, &gd.Termination, &gd.MovesUCI, &gd.BookPlies,
		); err != nil {
			return 0, nil, err
		}
		results = append(results, gd)
	}
	return total, results, rows.Err()
}

func (s *Store) ListEngines(ctx context.Context) ([]Engine, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, path, args, init
		FROM engines
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Engine
	for rows.Next() {
		var e Engine
		if err := rows.Scan(&e.ID, &e.Name, &e.Path, &e.Args, &e.Init); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) InsertEngine(ctx context.Context, e Engine) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO engines (name, path, args, init)
		VALUES (?, ?, ?, ?)
	`, e.Name, e.Path, e.Args, e.Init)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) UpdateEngine(ctx context.Context, e Engine) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE engines
		SET name = ?, path = ?, args = ?, init = ?
		WHERE id = ?
	`, e.Name, e.Path, e.Args, e.Init, e.ID)
	return err
}

func (s *Store) DeleteEngine(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM engines WHERE id = ?`, id)
	return err
}

func (s *Store) EngineInUse(ctx context.Context, id int64) (bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM games
		WHERE engine_white_id = ? OR engine_black_id = ?
	`, id, id)
	var count int
	if err := row.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *Store) EngineByID(ctx context.Context, id int64) (Engine, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, path, args, init
		FROM engines
		WHERE id = ?
	`, id)
	var e Engine
	if err := row.Scan(&e.ID, &e.Name, &e.Path, &e.Args, &e.Init); err != nil {
		return Engine{}, err
	}
	return e, nil
}

func (s *Store) EngineIDByName(ctx context.Context, name string) (int64, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id FROM engines WHERE name = ?`, name)
	var id int64
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) ListMatchups(ctx context.Context) ([]Matchup, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, engine_a_id, engine_b_id
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
		if err := rows.Scan(&m.ID, &m.EngineAID, &m.EngineBID); err != nil {
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
			INSERT OR IGNORE INTO matchups (engine_a_id, engine_b_id)
			VALUES (?, ?)
		`, m.EngineAID, m.EngineBID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DeleteMatchupsByEngine(ctx context.Context, engineID int64) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM matchups
		WHERE engine_a_id = ? OR engine_b_id = ?
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
		SELECT engine_white_id, COUNT(*) FROM games GROUP BY engine_white_id
		UNION ALL
		SELECT engine_black_id, COUNT(*) FROM games GROUP BY engine_black_id
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
		SELECT engine_a_id, COUNT(*) FROM matchups GROUP BY engine_a_id
		UNION ALL
		SELECT engine_b_id, COUNT(*) FROM matchups WHERE engine_b_id != engine_a_id GROUP BY engine_b_id
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
		WHERE engine_white_id = ? OR engine_black_id = ?
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
		out = append(out, res)
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
		SELECT g.engine_white_id, g.engine_black_id,
		       COALESCE(w.name, ''),
		       COALESCE(b.name, ''),
		       COALESCE(g.result, '*') as result,
		       COUNT(*)
		FROM games g
		LEFT JOIN engines w ON g.engine_white_id = w.id
		LEFT JOIN engines b ON g.engine_black_id = b.id
		GROUP BY g.engine_white_id, g.engine_black_id, result
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
		SELECT g.engine_white_id, g.engine_black_id,
		       COALESCE(w.name, ''),
		       COALESCE(b.name, ''),
		       g.movetime_ms, COALESCE(g.result, '*') as result, COUNT(*)
		FROM games g
		LEFT JOIN engines w ON g.engine_white_id = w.id
		LEFT JOIN engines b ON g.engine_black_id = b.id
		GROUP BY g.engine_white_id, g.engine_black_id, g.movetime_ms, result
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
		var count int
		if err := rows.Scan(&whiteID, &blackID, &white, &black, &movetime, &result, &count); err != nil {
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
		key := [3]int64{aID, bID, int64(movetime)}
		entry, ok := counts[key]
		if !ok {
			entry = &MatchupSummary{A: a, B: b, AID: aID, BID: bID, MovetimeMS: movetime}
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
		SELECT g.engine_white_id, g.engine_black_id, g.movetime_ms, COUNT(*)
		FROM games g
		GROUP BY g.engine_white_id, g.engine_black_id, g.movetime_ms
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MatchupCount
	for rows.Next() {
		var row MatchupCount
		if err := rows.Scan(&row.WhiteID, &row.BlackID, &row.MovetimeMS, &row.Count); err != nil {
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
		WHERE movetime_ms = ? AND ((engine_white_id = ? AND engine_black_id = ?) OR (engine_white_id = ? AND engine_black_id = ?))
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
		WHERE movetime_ms = ? AND ((engine_white_id = ? AND engine_black_id = ?) OR (engine_white_id = ? AND engine_black_id = ?))
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

// OpeningMovesLines returns one line per game for a specific opening key (first 2 plies).
func (s *Store) OpeningMovesLines(ctx context.Context, opening string) (string, error) {
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
		if openingKeyForMoves(moves) != opening {
			continue
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

func (s *Store) DeleteOpeningGames(ctx context.Context, opening string) (int64, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, moves_uci
		FROM games
	`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		var moves string
		if err := rows.Scan(&id, &moves); err != nil {
			return 0, err
		}
		if openingKeyForMoves(moves) == opening {
			ids = append(ids, id)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	deleted := int64(0)
	for _, id := range ids {
		res, err := s.db.ExecContext(ctx, `DELETE FROM games WHERE id = ?`, id)
		if err != nil {
			return deleted, err
		}
		count, err := res.RowsAffected()
		if err != nil {
			return deleted, err
		}
		deleted += count
	}
	return deleted, nil
}

func openingKeyForMoves(movesUCI string) string {
	if strings.TrimSpace(movesUCI) == "" {
		return "(no moves)"
	}
	parts := strings.Fields(movesUCI)
	if len(parts) >= 2 {
		return parts[0] + " " + parts[1]
	}
	return parts[0]
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
