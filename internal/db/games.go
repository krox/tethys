package db

import "context"

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

// list most recent finished games
func (s *Store) ListFinishedGames(ctx context.Context, limit int) ([]GameDetail, error) {
	var out []GameDetail
	err := s.db.SelectContext(ctx, &out, `
		SELECT g.id,
			g.played_at,
			w.name AS white,
			b.name AS black,
			r.movetime_ms,
			CASE WHEN g.result = '' THEN '*' ELSE g.result END AS result,
			g.termination AS termination,
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

func (s *Store) ListFinishedGamesMoves(ctx context.Context, limit int) ([]GameMovesRow, error) {
	var out []GameMovesRow
	err := s.db.SelectContext(ctx, &out, `
		SELECT moves_uci,
			CASE WHEN result = '' THEN '*' ELSE result END AS result
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
			CASE WHEN result = '' THEN '*' ELSE result END AS result
		FROM games
		ORDER BY id ASC
	`)
	return out, err
}

func (s *Store) GameMoves(ctx context.Context, id int64) (moves, result string, err error) {
	var row struct {
		MovesUCI string `db:"moves_uci"`
		Result   string `db:"result"`
	}
	if err := s.db.GetContext(ctx, &row, `SELECT moves_uci, result FROM games WHERE id = ?`, id); err != nil {
		return "", "", err
	}
	result = row.Result
	if result == "" {
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
			CASE WHEN g.result = '' THEN '*' ELSE g.result END AS result,
			g.termination AS termination,
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
		where += " AND (CASE WHEN result = '' THEN '*' ELSE result END) = ?"
		args = append(args, filter.Result)
	}
	if filter.Termination != "" {
		where += " AND termination = ?"
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
			CASE WHEN g.result = '' THEN '*' ELSE g.result END AS result,
			g.termination AS termination,
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

func (s *Store) ListResults(ctx context.Context) ([]string, error) {
	var raw []string
	err := s.db.SelectContext(ctx, &raw, `
		SELECT DISTINCT CASE WHEN result = '' THEN '*' ELSE result END AS result
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
		SELECT DISTINCT termination
		FROM games
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
			CASE WHEN g.result = '' THEN '*' ELSE g.result END AS result,
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
			CASE WHEN g.result = '' THEN '*' ELSE g.result END AS result,
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
		SELECT CASE WHEN result = '' THEN '*' ELSE result END AS result,
			termination,
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
			CASE WHEN result = '' THEN '*' ELSE result END AS result
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
			CASE WHEN result = '' THEN '*' ELSE result END AS result
		FROM games
		WHERE (CASE WHEN result = '' THEN '*' ELSE result END) = ? AND termination = ?
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
		WHERE (CASE WHEN result = '' THEN '*' ELSE result END) = ? AND termination = ?
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
			CASE WHEN result = '' THEN '*' ELSE result END AS result
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
