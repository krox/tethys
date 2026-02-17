package db

import (
	"context"
	"strings"
)

// list all engines
func (s *Store) ListEngines(ctx context.Context) ([]Engine, error) {
	var out []Engine
	err := s.db.SelectContext(ctx, &out, `
		SELECT id, name, engine_path, engine_args, engine_init, engine_elo
		FROM players
		ORDER BY engine_elo DESC, id ASC
	`)
	return out, err
}

// add new engine, returning its newly assigned ID
func (s *Store) InsertEngine(ctx context.Context, e Engine) (int64, error) {
	e.Path = strings.TrimSpace(e.Path)
	res, err := s.db.NamedExecContext(ctx, `
		INSERT INTO players (name, engine_path, engine_args, engine_init)
		VALUES (:name, :engine_path, :engine_args, :engine_init)
	`, e)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// find an engine by its ID
func (s *Store) EngineByID(ctx context.Context, id int64) (Engine, error) {
	var e Engine
	err := s.db.GetContext(ctx, &e, `
		SELECT id, name, engine_path, engine_args, engine_init, engine_elo
		FROM players
		WHERE id = ?
	`, id)
	return e, err
}

// find an engine by its ID and update its details
func (s *Store) UpdateEngine(ctx context.Context, e Engine) error {
	e.Path = strings.TrimSpace(e.Path)
	_, err := s.db.NamedExecContext(ctx, `
		UPDATE players
		SET name = :name,
			engine_path = :engine_path,
			engine_args = :engine_args,
			engine_init = :engine_init
		WHERE id = :id
	`, e)
	return err
}

// delete a single engine by its ID
func (s *Store) DeleteEngine(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM players WHERE id = ?`, id)
	return err
}

// replace all engines ELO ratings
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

// lookup engine ID by name
func (s *Store) EngineIDByName(ctx context.Context, name string) (int64, error) {
	var id int64
	err := s.db.GetContext(ctx, &id, `SELECT id FROM players WHERE name = ?`, name)
	return id, err
}

// TODO: I dont think this function is quite correct w.r.t. mirror matchups
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

func (s *Store) CountEngines(ctx context.Context) (int, error) {
	var count int
	err := s.db.GetContext(ctx, &count, `
		SELECT COUNT(*)
		FROM players
		WHERE engine_path != ''
	`)
	return count, err
}
