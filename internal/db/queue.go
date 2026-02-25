package db

import (
	"context"
	"database/sql"
)

func (s *Store) EnqueueGames(ctx context.Context, entries []GameQueueEntry) error {
	if len(entries) == 0 {
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
		INSERT INTO game_queue (white_player_id, black_player_id, movetime_ms, book_path)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, entry := range entries {
		if _, err = stmt.ExecContext(ctx, entry.WhiteID, entry.BlackID, entry.MovetimeMS, entry.BookPath); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Store) DequeueGame(ctx context.Context) (GameQueueEntry, bool, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return GameQueueEntry{}, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var entry GameQueueEntry
	if err = tx.GetContext(ctx, &entry, `
		SELECT id, created_at, white_player_id, black_player_id, movetime_ms, book_path
		FROM game_queue
		ORDER BY id ASC
		LIMIT 1
	`); err != nil {
		if err == sql.ErrNoRows {
			_ = tx.Rollback()
			return GameQueueEntry{}, false, nil
		}
		return GameQueueEntry{}, false, err
	}

	if _, err = tx.ExecContext(ctx, `DELETE FROM game_queue WHERE id = ?`, entry.ID); err != nil {
		return GameQueueEntry{}, false, err
	}

	if err = tx.Commit(); err != nil {
		return GameQueueEntry{}, false, err
	}
	return entry, true, nil
}

func (s *Store) ClearGameQueue(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM game_queue`)
	return err
}

func (s *Store) GameQueueSize(ctx context.Context) (int, error) {
	var count int
	err := s.db.GetContext(ctx, &count, `SELECT COUNT(*) FROM game_queue`)
	return count, err
}

func (s *Store) ListGameQueue(ctx context.Context, limit int) ([]GameQueueRow, error) {
	if limit <= 0 {
		limit = 10
	}
	var rows []GameQueueRow
	err := s.db.SelectContext(ctx, &rows, `
		SELECT q.id,
			q.created_at,
			q.white_player_id,
			q.black_player_id,
			w.name AS white,
			b.name AS black,
			q.movetime_ms,
			q.book_path
		FROM game_queue q
		LEFT JOIN players w ON q.white_player_id = w.id
		LEFT JOIN players b ON q.black_player_id = b.id
		ORDER BY q.id ASC
		LIMIT ?
	`, limit)
	return rows, err
}
