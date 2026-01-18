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
		`CREATE TABLE IF NOT EXISTS games (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			played_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			engine_white TEXT NOT NULL,
			engine_black TEXT NOT NULL,
			movetime_ms INTEGER NOT NULL,
			result TEXT,         -- 1-0|0-1|1/2-1/2|*
			termination TEXT,
			moves_uci TEXT NOT NULL DEFAULT '',
			book_plies INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE INDEX IF NOT EXISTS idx_games_played_at ON games(played_at);`,
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
	ID         int64
	PlayedAt   string
	White      string
	Black      string
	MovetimeMS int
	Result     sql.NullString
	Termination sql.NullString
	MovesUCI   string
	BookPlies  int
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
	Engine      string
	White       string
	Black       string
	AllowSwap   bool
	MovetimeMS  int
	Result      string
	Termination string
}

func (s *Store) InsertFinishedGame(ctx context.Context, white, black string, movetimeMS int, result, termination, movesUCI string, bookPlies int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO games (engine_white, engine_black, movetime_ms, result, termination, moves_uci, book_plies)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, white, black, movetimeMS, result, termination, movesUCI, bookPlies)
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
		SELECT id, played_at, engine_white, engine_black, movetime_ms, result, termination, moves_uci, book_plies
		FROM games
		ORDER BY id DESC
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
		SELECT id, played_at, engine_white, engine_black, movetime_ms, result, termination, moves_uci, book_plies
		FROM games
		ORDER BY id DESC
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
	EngineA string
	EngineB string
	WinsA   int
	WinsB   int
	Draws   int
}

type MatchupSummary struct {
	A          string
	B          string
	MovetimeMS int
	WinsA      int
	WinsB      int
	Draws      int
}

type MatchupCount struct {
	White      string
	Black      string
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
		SELECT id, played_at, engine_white, engine_black, movetime_ms,
		       COALESCE(result, '*'), COALESCE(termination, ''), moves_uci, book_plies
		FROM games
		WHERE id = ?
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
	if filter.White != "" && filter.Black != "" {
		if filter.AllowSwap {
			where += " AND ((engine_white = ? AND engine_black = ?) OR (engine_white = ? AND engine_black = ?))"
			args = append(args, filter.White, filter.Black, filter.Black, filter.White)
		} else {
			where += " AND engine_white = ? AND engine_black = ?"
			args = append(args, filter.White, filter.Black)
		}
	} else if filter.White != "" {
		where += " AND engine_white = ?"
		args = append(args, filter.White)
	} else if filter.Black != "" {
		where += " AND engine_black = ?"
		args = append(args, filter.Black)
	}
	if filter.Engine != "" {
		where += " AND (engine_white = ? OR engine_black = ?)"
		args = append(args, filter.Engine, filter.Engine)
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
		SELECT id, played_at, engine_white, engine_black, movetime_ms,
		       COALESCE(result, '*'), COALESCE(termination, ''), moves_uci, book_plies
		FROM games
		` + where + `
		ORDER BY id DESC
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

func (s *Store) ListEngines(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT engine_white FROM games
		UNION
		SELECT DISTINCT engine_black FROM games
		ORDER BY 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "" {
			out = append(out, name)
		}
	}
	return out, rows.Err()
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
		SELECT engine_white, engine_black, COALESCE(result, '*') as result, COUNT(*)
		FROM games
		GROUP BY engine_white, engine_black, result
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[[2]string]*PairResult)
	for rows.Next() {
		var white, black, result string
		var count int
		if err := rows.Scan(&white, &black, &result, &count); err != nil {
			return nil, err
		}
		if result != "1-0" && result != "0-1" && result != "1/2-1/2" {
			continue
		}
		a, b := white, black
		swap := false
		if a > b {
			a, b = b, a
			swap = true
		}
		key := [2]string{a, b}
		entry, ok := counts[key]
		if !ok {
			entry = &PairResult{EngineA: a, EngineB: b}
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
		SELECT engine_white, engine_black, movetime_ms, COALESCE(result, '*') as result, COUNT(*)
		FROM games
		GROUP BY engine_white, engine_black, movetime_ms, result
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[[3]string]*MatchupSummary)
	for rows.Next() {
		var white, black, result string
		var movetime int
		var count int
		if err := rows.Scan(&white, &black, &movetime, &result, &count); err != nil {
			return nil, err
		}
		if result != "1-0" && result != "0-1" && result != "1/2-1/2" {
			continue
		}
		a, b := white, black
		swap := false
		if a > b {
			a, b = b, a
			swap = true
		}
		key := [3]string{a, b, fmt.Sprintf("%d", movetime)}
		entry, ok := counts[key]
		if !ok {
			entry = &MatchupSummary{A: a, B: b, MovetimeMS: movetime}
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
		SELECT engine_white, engine_black, movetime_ms, COUNT(*)
		FROM games
		GROUP BY engine_white, engine_black, movetime_ms
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MatchupCount
	for rows.Next() {
		var row MatchupCount
		if err := rows.Scan(&row.White, &row.Black, &row.MovetimeMS, &row.Count); err != nil {
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
func (s *Store) MatchupMovesLines(ctx context.Context, a, b string, movetimeMS int) (string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT moves_uci, result
		FROM games
		WHERE movetime_ms = ? AND ((engine_white = ? AND engine_black = ?) OR (engine_white = ? AND engine_black = ?))
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

func (s *Store) DeleteMatchupGames(ctx context.Context, a, b string, movetimeMS int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM games
		WHERE movetime_ms = ? AND ((engine_white = ? AND engine_black = ?) OR (engine_white = ? AND engine_black = ?))
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
