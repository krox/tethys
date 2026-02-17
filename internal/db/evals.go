package db

import "context"

func (s *Store) EvalByZobrist(ctx context.Context, key uint64) (Eval, error) {
	var e Eval
	err := s.db.GetContext(ctx, &e, `
		SELECT zobrist_key, fen, score, pv, engine_id, depth
		FROM evals
		WHERE zobrist_key = ?
	`, key)
	return e, err
}

func (s *Store) UpsertEval(ctx context.Context, e Eval) error {
	_, err := s.db.NamedExecContext(ctx, `
		INSERT INTO evals (zobrist_key, fen, score, pv, engine_id, depth)
		VALUES (:zobrist_key, :fen, :score, :pv, :engine_id, :depth)
		ON CONFLICT(zobrist_key) DO UPDATE SET
			fen = excluded.fen,
			score = excluded.score,
			pv = excluded.pv,
			engine_id = excluded.engine_id,
			depth = excluded.depth
	`, e)
	return err
}
