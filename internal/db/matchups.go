package db

import "context"

// list all existing matchups
func (s *Store) ListMatchups(ctx context.Context) ([]Matchup, error) {
	var out []Matchup
	err := s.db.SelectContext(ctx, &out, `
		SELECT player_a_id, player_b_id
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
			INSERT OR IGNORE INTO matchups (player_a_id, player_b_id)
			VALUES (?, ?)
		`, m.PlayerAID, m.PlayerBID)
		if err != nil {
			return err
		}
	}
	return nil
}

// delete all matchups involving the given engine
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
