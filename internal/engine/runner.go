package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/notnil/chess"

	"tethys/internal/book"
	"tethys/internal/configstore"
	"tethys/internal/db"
)

type LiveState struct {
	GameID     int64
	CreatedAt  string
	White      string
	Black      string
	MovetimeMS int
	Status     string
	Result     string
	MovesUCI   []string
	FEN        string
	Board      [][]SquareView
	UpdatedAt  time.Time
}

type SquareView struct {
	Glyph string
	Class string
}

type Runner struct {
	store    *db.Store
	config   *configstore.Store
	b        *Broadcaster
	seq      int64
	pickIdx  int
	bookMu   sync.Mutex
	bookPath string
	bookMod  time.Time
	book     *book.Book

	mu      sync.RWMutex
	live    LiveState
	stop    chan struct{}
	restart chan struct{}

	runningMu sync.Mutex
	running   bool
}

func NewRunner(store *db.Store, config *configstore.Store, b *Broadcaster) *Runner {
	start := chess.StartingPosition()
	r := &Runner{
		store:   store,
		config:  config,
		b:       b,
		stop:    make(chan struct{}),
		restart: make(chan struct{}, 1),
		live:    LiveState{Status: "starting", FEN: start.String(), Board: boardFromPosition(start)},
	}
	if store != nil {
		if latest, err := store.LatestGame(context.Background()); err == nil {
			r.seq = latest.ID
		} else if err != sql.ErrNoRows {
			log.Printf("runner: latest game lookup failed: %v", err)
		}
	}
	return r
}

func (r *Runner) Start(ctx context.Context) {
	r.runningMu.Lock()
	if r.running {
		r.runningMu.Unlock()
		return
	}
	r.running = true
	r.runningMu.Unlock()

	go r.loop(ctx)
}

func (r *Runner) Stop() {
	select {
	case <-r.stop:
		return
	default:
		close(r.stop)
	}
}

func (r *Runner) Restart() {
	select {
	case r.restart <- struct{}{}:
	default:
	}
}

func (r *Runner) Live() LiveState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copyMoves := append([]string(nil), r.live.MovesUCI...)
	copyBoard := make([][]SquareView, len(r.live.Board))
	for i := range r.live.Board {
		copyBoard[i] = append([]SquareView(nil), r.live.Board[i]...)
	}
	ls := r.live
	ls.MovesUCI = copyMoves
	ls.Board = copyBoard
	return ls
}

func (r *Runner) setLive(update func(*LiveState)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	update(&r.live)
	r.live.UpdatedAt = time.Now()
}

func (r *Runner) loop(parent context.Context) {
	for {
		select {
		case <-r.stop:
			return
		default:
		}

		ctx, cancel := context.WithCancel(parent)
		func() {
			defer cancel()

			cfg, err := r.config.GetConfig(ctx)
			if err != nil {
				log.Printf("runner: config error: %v", err)
				r.setLive(func(ls *LiveState) {
					ls.Status = "error"
					ls.Result = "config error"
				})
				time.Sleep(2 * time.Second)
				return
			}

			counts := []db.MatchupCount{}
			if r.store != nil {
				if rows, err := r.store.ListMatchupCounts(ctx); err == nil {
					counts = rows
				} else {
					log.Printf("runner: matchup count error: %v", err)
				}
			}

			assignment, nextIdx := selectAssignment(cfg, counts, r.pickIdx)
			r.pickIdx = nextIdx

			if assignment.White.Path == "" || assignment.Black.Path == "" {
				start := chess.StartingPosition()
				r.setLive(func(ls *LiveState) {
					ls.Status = "idle"
					ls.Result = "configure engines in /admin"
					ls.FEN = start.String()
					ls.Board = boardFromPosition(start)
				})
				time.Sleep(2 * time.Second)
				return
			}

			whiteDisplay := assignment.WhiteName
			blackDisplay := assignment.BlackName

			r.seq++
			r.setLive(func(ls *LiveState) {
				ls.GameID = r.seq
				ls.White = whiteDisplay
				ls.Black = blackDisplay
				ls.MovetimeMS = assignment.MovetimeMS
				ls.Status = "running"
				ls.Result = "*"
				ls.MovesUCI = nil
			})
			r.b.Publish()

			whiteArgs := strings.Fields(assignment.White.Args)
			blackArgs := strings.Fields(assignment.Black.Args)

			white := NewUCIEngine(assignment.White.Path, whiteArgs)
			selfplay := assignment.WhiteName == assignment.BlackName
			var black *UCIEngine
			if selfplay {
				black = white
			} else {
				black = NewUCIEngine(assignment.Black.Path, blackArgs)
			}

			if err := white.Start(ctx); err != nil {
				r.failGame(ctx, "*", fmt.Sprintf("white start error: %v", err))
				return
			}
			defer func() { _ = white.Close() }()

			if !selfplay {
				if err := black.Start(ctx); err != nil {
					r.failGame(ctx, "*", fmt.Sprintf("black start error: %v", err))
					return
				}
				defer func() { _ = black.Close() }()
			}

			if err := applyInit(ctx, white, assignment.White.Init); err != nil {
				r.failGame(ctx, "*", fmt.Sprintf("white init error: %v", err))
				return
			}

			if selfplay {
				if err := applyInit(ctx, black, assignment.White.Init); err != nil {
					r.failGame(ctx, "*", fmt.Sprintf("black init error: %v", err))
					return
				}
			} else {
				if err := applyInit(ctx, black, assignment.Black.Init); err != nil {
					r.failGame(ctx, "*", fmt.Sprintf("black init error: %v", err))
					return
				}
			}

			if err := white.NewGame(ctx); err != nil {
				r.failGame(ctx, "*", fmt.Sprintf("white newgame error: %v", err))
				return
			}
			if !selfplay {
				if err := black.NewGame(ctx); err != nil {
					r.failGame(ctx, "*", fmt.Sprintf("black newgame error: %v", err))
					return
				}
			}

			game := chess.NewGame()
			r.setLive(func(ls *LiveState) {
				ls.FEN = game.Position().String()
				ls.Board = boardFromPosition(game.Position())
			})
			r.b.Publish()
			movesUCI := make([]string, 0, 256)
			bookPlies := 0

			for {
				select {
				case <-r.stop:
					r.failGame(ctx, "*", "service stopping")
					return
				case <-r.restart:
					r.failGame(ctx, "*", "restarted by admin")
					return
				default:
				}

				if assignment.MaxPlies > 0 && len(movesUCI) >= assignment.MaxPlies {
					result := "1/2-1/2"
					termination := "Max plies"
					_, err := r.store.InsertFinishedGame(ctx, whiteDisplay, blackDisplay, assignment.MovetimeMS, result, termination, strings.Join(movesUCI, " "), bookPlies)
					if err != nil {
						log.Printf("runner: insert game error: %v", err)
					}
					r.setLive(func(ls *LiveState) {
						ls.Status = "finished"
						ls.Result = result
					})
					r.b.Publish()
					return
				}

				if game.Outcome() != chess.NoOutcome {
					result, termination := outcomeToResult(game)
					_, err := r.store.InsertFinishedGame(ctx, whiteDisplay, blackDisplay, assignment.MovetimeMS, result, termination, strings.Join(movesUCI, " "), bookPlies)
					if err != nil {
						log.Printf("runner: insert game error: %v", err)
					}
					r.setLive(func(ls *LiveState) {
						ls.Status = "finished"
						ls.Result = result
					})
					r.b.Publish()
					return
				}

				isWhiteToMove := game.Position().Turn() == chess.White
				var eng *UCIEngine
				if isWhiteToMove {
					eng = white
				} else {
					eng = black
				}

				best, ok := r.bookMove(game.Position(), len(movesUCI), assignment)
				var err error
				if !ok {
					best, err = eng.BestMoveMovetime(ctx, movesUCI, assignment.MovetimeMS)
					if err != nil {
						r.failGame(ctx, "*", fmt.Sprintf("bestmove error: %v", err))
						return
					}
				} else {
					bookPlies++
				}
				if best == "(none)" || best == "0000" {
					r.failGame(ctx, "*", "engine returned no move")
					return
				}

				n := chess.UCINotation{}
				mv, err := n.Decode(game.Position(), best)
				if err != nil {
					r.failGame(ctx, "*", fmt.Sprintf("illegal move from engine: %s (%v)", best, err))
					return
				}

				if err := game.Move(mv); err != nil {
					r.failGame(ctx, "*", fmt.Sprintf("move apply error: %s (%v)", best, err))
					return
				}

				movesUCI = append(movesUCI, best)
				r.setLive(func(ls *LiveState) {
					ls.MovesUCI = append([]string(nil), movesUCI...)
					ls.FEN = game.Position().String()
					ls.Board = boardFromPosition(game.Position())
				})
				r.b.Publish()
			}
		}()

		// Small pause between games, even on failure.
		time.Sleep(200 * time.Millisecond)
	}
}

type matchupCandidate struct {
	White string
	Black string
}

func selectAssignment(cfg configstore.Config, counts []db.MatchupCount, pickIdx int) (configstore.ColorAssignment, int) {
	assign := configstore.ColorAssignment{
		MovetimeMS:   cfg.MovetimeMS,
		MaxPlies:     cfg.MaxPlies,
		BookEnabled:  cfg.BookEnabled,
		BookPath:     cfg.BookPath,
		BookMaxPlies: cfg.BookMaxPlies,
	}
	if assign.MovetimeMS <= 0 {
		assign.MovetimeMS = 100
	}
	if assign.MaxPlies <= 0 {
		assign.MaxPlies = 200
	}
	if assign.BookMaxPlies <= 0 {
		assign.BookMaxPlies = 16
	}

	engineByName := make(map[string]configstore.EngineConfig)
	for _, e := range cfg.Engines {
		if e.Name == "" || e.Path == "" {
			continue
		}
		engineByName[e.Name] = e
	}

	validPairs := make([]configstore.PairConfig, 0, len(cfg.EnabledPairs))
	for _, p := range cfg.EnabledPairs {
		if p.A == "" || p.B == "" {
			continue
		}
		if _, ok := engineByName[p.A]; !ok {
			continue
		}
		if _, ok := engineByName[p.B]; !ok {
			continue
		}
		validPairs = append(validPairs, p)
	}

	if len(validPairs) == 0 {
		return assign, 0
	}

	countMap := make(map[string]int)
	for _, c := range counts {
		key := fmt.Sprintf("%s\x00%s\x00%d", c.White, c.Black, c.MovetimeMS)
		countMap[key] = c.Count
	}

	candidates := make([]matchupCandidate, 0, len(validPairs)*2)
	for _, p := range validPairs {
		if p.A == p.B {
			candidates = append(candidates, matchupCandidate{White: p.A, Black: p.A})
			continue
		}
		candidates = append(candidates, matchupCandidate{White: p.A, Black: p.B})
		candidates = append(candidates, matchupCandidate{White: p.B, Black: p.A})
	}

	minCount := -1
	for _, c := range candidates {
		key := fmt.Sprintf("%s\x00%s\x00%d", c.White, c.Black, assign.MovetimeMS)
		count := countMap[key]
		if minCount == -1 || count < minCount {
			minCount = count
		}
	}

	filtered := make([]matchupCandidate, 0, len(candidates))
	for _, c := range candidates {
		key := fmt.Sprintf("%s\x00%s\x00%d", c.White, c.Black, assign.MovetimeMS)
		if countMap[key] == minCount {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		filtered = candidates
	}

	idx := pickIdx
	if idx < 0 || idx >= len(filtered) {
		idx = 0
	}
	chosen := filtered[idx]

	white := engineByName[chosen.White]
	black := engineByName[chosen.Black]
	assign.White, assign.Black = white, black
	assign.WhiteName, assign.BlackName = chosen.White, chosen.Black

	nextIdx := (idx + 1) % len(filtered)
	return assign, nextIdx
}

func (r *Runner) failGame(ctx context.Context, result, termination string) {
	r.setLive(func(ls *LiveState) {
		ls.Status = "finished"
		ls.Result = result
	})
	r.b.Publish()
}

func (r *Runner) bookMove(pos *chess.Position, ply int, assignment configstore.ColorAssignment) (string, bool) {
	if !assignment.BookEnabled || assignment.BookPath == "" {
		return "", false
	}
	if assignment.BookMaxPlies > 0 && ply >= assignment.BookMaxPlies {
		return "", false
	}

	bookObj, err := r.loadBook(assignment.BookPath)
	if err != nil || bookObj == nil {
		return "", false
	}

	move, ok := bookObj.Lookup(pos)
	return move, ok
}

func (r *Runner) loadBook(path string) (*book.Book, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	r.bookMu.Lock()
	defer r.bookMu.Unlock()
	if r.book != nil && r.bookPath == path && r.bookMod.Equal(info.ModTime()) {
		return r.book, nil
	}

	b, err := book.Load(path)
	if err != nil {
		return nil, err
	}
	r.book = b
	r.bookPath = path
	r.bookMod = info.ModTime()
	return r.book, nil
}

func applyInit(ctx context.Context, e *UCIEngine, init string) error {
	lines := strings.Split(init, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if err := e.Send(line); err != nil {
			return err
		}
	}
	return e.IsReady(ctx)
}

func outcomeToResult(g *chess.Game) (result, termination string) {
	out := g.Outcome()
	method := g.Method()

	switch out {
	case chess.WhiteWon:
		result = "1-0"
	case chess.BlackWon:
		result = "0-1"
	case chess.Draw:
		result = "1/2-1/2"
	default:
		result = "*"
	}
	termination = method.String()
	return result, termination
}

func engineDisplayName(path string, fallback string) string {
	base := filepath.Base(path)
	if base == "." || base == "/" || base == "" {
		return fallback
	}
	return base
}

func boardFromPosition(pos *chess.Position) [][]SquareView {
	board := make([][]SquareView, 0, 8)
	b := pos.Board()

	for r := chess.Rank8; r >= chess.Rank1; r-- {
		row := make([]SquareView, 0, 8)
		for f := chess.FileA; f <= chess.FileH; f++ {
			sq := chess.NewSquare(f, r)
			p := b.Piece(sq)
			glyph := pieceGlyph(p)

			// a1 is dark.
			fileIdx := int(f)
			rankIdx := int(r)
			light := (fileIdx+rankIdx)%2 == 1
			class := "sq "
			if light {
				class += "light"
			} else {
				class += "dark"
			}

			row = append(row, SquareView{Glyph: glyph, Class: class})
		}
		board = append(board, row)
	}
	return board
}

func pieceGlyph(p chess.Piece) string {
	if p == chess.NoPiece {
		return ""
	}

	isWhite := p.Color() == chess.White
	switch p.Type() {
	case chess.King:
		if isWhite {
			return "♔"
		}
		return "♚"
	case chess.Queen:
		if isWhite {
			return "♕"
		}
		return "♛"
	case chess.Rook:
		if isWhite {
			return "♖"
		}
		return "♜"
	case chess.Bishop:
		if isWhite {
			return "♗"
		}
		return "♝"
	case chess.Knight:
		if isWhite {
			return "♘"
		}
		return "♞"
	case chess.Pawn:
		if isWhite {
			return "♙"
		}
		return "♟"
	default:
		return ""
	}
}
