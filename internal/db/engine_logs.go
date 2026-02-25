package db

import "context"

type EngineLog struct {
	GameID    int64  `db:"game_id"`
	Ply       int    `db:"ply"`
	EngineID  int64  `db:"engine_id"`
	ElapsedMS int64  `db:"elapsed_ms"`
	Log       string `db:"log"`
}

func (s *Store) InsertEngineLogs(ctx context.Context, gameID int64, logs []EngineLog) error {
	if len(logs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR REPLACE INTO engine_logs (game_id, ply, engine_id, elapsed_ms, log)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, entry := range logs {
		if _, err = stmt.ExecContext(ctx, gameID, entry.Ply, entry.EngineID, entry.ElapsedMS, entry.Log); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Store) ListEngineLogsByGame(ctx context.Context, gameID int64) ([]EngineLog, error) {
	var out []EngineLog
	err := s.db.SelectContext(ctx, &out, `
		SELECT game_id, ply, engine_id, elapsed_ms, log
		FROM engine_logs
		WHERE game_id = ?
		ORDER BY ply ASC, engine_id ASC
	`, gameID)
	return out, err
}
