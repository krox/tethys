package db

type EngineLog struct {
	GameID    int64  `db:"game_id"`
	Ply       int    `db:"ply"`
	EngineID  int64  `db:"engine_id"`
	ElapsedMS int64  `db:"elapsed_ms"`
	Log       string `db:"log"`
}
