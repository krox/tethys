package db

import (
	"context"
	"strings"
)

func (s *Store) InsertRuleset(ctx context.Context, movetimeMS int, bookPath string, bookMaxPlies int) (int64, error) {
	params := Ruleset{
		MovetimeMS:   movetimeMS,
		BookPath:     strings.TrimSpace(bookPath),
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

func (s *Store) DeleteRuleset(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM rulesets WHERE id = ?`, id)
	return err
}

func (s *Store) ListRulesets(ctx context.Context) ([]Ruleset, error) {
	var out []Ruleset
	err := s.db.SelectContext(ctx, &out, `
		SELECT id,
			movetime_ms,
			book_path,
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
			book_path,
			book_max_plies
		FROM rulesets
		WHERE id = ?
	`, id)
	return r, err
}
