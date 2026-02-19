package db

type Settings struct {
	OpeningMin       int    `db:"opening_min"`
	AnalysisEngineID int64  `db:"analysis_engine_id"`
	AnalysisDepth    int    `db:"analysis_depth"`
	GameMovetimeMS   int    `db:"game_movetime_ms"`
	GameBookPath     string `db:"game_book_path"`
}

type GameDetail struct {
	ID          int64  `db:"id"`
	PlayedAt    string `db:"played_at"`
	White       string `db:"white"`
	Black       string `db:"black"`
	MovetimeMS  int    `db:"movetime_ms"`
	Result      string `db:"result"`
	Termination string `db:"termination"`
	MovesUCI    string `db:"moves_uci"`
	Plies       int    `db:"ply_count"`
	BookPlies   int    `db:"book_plies"`
}

type Eval struct {
	ZobristKey uint64 `db:"zobrist_key"`
	FEN        string `db:"fen"`
	Score      string `db:"score"`
	PV         string `db:"pv"`
	EngineID   int64  `db:"engine_id"`
	Depth      int    `db:"depth"`
}

type Engine struct {
	ID   int64   `db:"id"`
	Name string  `db:"name"`
	Path string  `db:"engine_path"`
	Args string  `db:"engine_args"`
	Init string  `db:"engine_init"`
	Elo  float64 `db:"engine_elo"`
}

type Matchup struct {
	ID        int64 `db:"id"`
	PlayerAID int64 `db:"player_a_id"`
	PlayerBID int64 `db:"player_b_id"`
}

type GameSearchFilter struct {
	EngineID    int64
	WhiteID     int64
	BlackID     int64
	AllowSwap   bool
	MovetimeMS  int
	Result      string
	Termination string
}

type GameMovesRow struct {
	MovesUCI string `db:"moves_uci"`
	Result   string `db:"result"`
}

type PairResult struct {
	EngineAID int64
	EngineBID int64
	EngineA   string
	EngineB   string
	WinsA     int
	WinsB     int
	Draws     int
}

type MatchupSummary struct {
	AID        int64
	BID        int64
	A          string
	B          string
	MovetimeMS int
	WinsA      int
	WinsB      int
	Draws      int
}

type MatchupCount struct {
	WhiteID int64 `db:"white_id"`
	BlackID int64 `db:"black_id"`
	Count   int   `db:"count"`
}

type ResultSummary struct {
	Result      string `db:"result"`
	Termination string `db:"termination"`
	Count       int    `db:"count"`
}
