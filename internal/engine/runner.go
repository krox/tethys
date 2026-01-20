package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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

	mu   sync.RWMutex
	live LiveState
	stop chan struct{}

	runningMu sync.Mutex
	running   bool
}

func NewRunner(store *db.Store, config *configstore.Store, b *Broadcaster) *Runner {
	start := chess.StartingPosition()
	r := &Runner{
		store:  store,
		config: config,
		b:      b,
		stop:   make(chan struct{}),
		live:   LiveState{Status: "starting", FEN: start.String(), Board: boardFromPosition(start)},
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

func (r *Runner) failGame(ctx context.Context, result, termination string) {
	r.setLive(func(ls *LiveState) {
		ls.Status = "finished"
		ls.Result = result
	})
	r.b.Publish()
}
