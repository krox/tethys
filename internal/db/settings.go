package db

import (
	"context"
	"strconv"
)

func (s *Store) GetSettings(ctx context.Context) (Settings, error) {
	defaults := Settings{
		OpeningMin:       20,
		AnalysisEngineID: 0,
		AnalysisDepth:    12,
	}
	rows := []struct {
		Key   string `db:"key"`
		Value string `db:"value"`
	}{}
	if err := s.db.SelectContext(ctx, &rows, `
		SELECT key, CAST(value AS TEXT) AS value
		FROM settings
	`); err != nil {
		return Settings{}, err
	}
	settings := defaults
	for _, row := range rows {
		switch row.Key {
		case "opening_min":
			if v, err := strconv.Atoi(row.Value); err == nil {
				settings.OpeningMin = v
			}
		case "analysis_engine_id":
			if v, err := strconv.ParseInt(row.Value, 10, 64); err == nil {
				settings.AnalysisEngineID = v
			}
		case "analysis_depth":
			if v, err := strconv.Atoi(row.Value); err == nil {
				settings.AnalysisDepth = v
			}
		}
	}
	return settings, nil
}

func (s *Store) UpdateSettings(ctx context.Context, settings Settings) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	upsert := `INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`
	if _, err = tx.ExecContext(ctx, upsert, "opening_min", settings.OpeningMin); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, upsert, "analysis_engine_id", settings.AnalysisEngineID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, upsert, "analysis_depth", settings.AnalysisDepth); err != nil {
		return err
	}

	return tx.Commit()
}
