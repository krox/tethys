package engine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/notnil/chess"

	"tethys/internal/book"
	"tethys/internal/db"
	"tethys/internal/ranking"
)

type LiveState struct {
	CreatedAt  string
	White      string
	Black      string
	MovetimeMS int
	Status     string
	Result     string
	MovesUCI   []string
	BookPlies  int
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
	b        *Broadcaster
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

func NewRunner(store *db.Store, b *Broadcaster) *Runner {
	start := chess.StartingPosition()
	r := &Runner{
		store: store,
		b:     b,
		stop:  make(chan struct{}),
		live:  LiveState{Status: "starting", FEN: start.String(), Board: boardFromPosition(start)},
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

			engines := []db.Engine{}
			settings := db.Settings{}
			if r.store != nil {
				if rows, err := r.store.ListEngines(ctx); err == nil {
					engines = rows
				} else {
					log.Printf("runner: engine list error: %v", err)
				}
				if cfg, err := r.store.GetSettings(ctx); err == nil {
					settings = cfg
				} else {
					log.Printf("runner: settings error: %v", err)
				}
			}

			engineByID := make(map[int64]db.Engine)
			for _, e := range engines {
				if e.ID == 0 || e.Name == "" || e.Path == "" {
					continue
				}
				engineByID[e.ID] = e
			}

			var assignment ColorAssignment
			var ok bool
			if r.store != nil {
				if entry, hasEntry, err := r.store.DequeueGame(ctx); err != nil {
					log.Printf("runner: dequeue game error: %v", err)
				} else if hasEntry {
					assignment, ok = assignmentFromQueue(entry, engineByID)
				}
				if !ok {
					if err := r.fillGameQueue(ctx, settings); err != nil {
						log.Printf("runner: fill queue error: %v", err)
					}
					if entry, hasEntry, err := r.store.DequeueGame(ctx); err != nil {
						log.Printf("runner: dequeue game error: %v", err)
					} else if hasEntry {
						assignment, ok = assignmentFromQueue(entry, engineByID)
					}
				}
			}

			if !ok || assignment.White.Path == "" || assignment.Black.Path == "" {
				start := chess.StartingPosition()
				message := "waiting for queue"
				if len(engineByID) < 2 {
					message = "configure engines in /admin"
				}
				r.setLive(func(ls *LiveState) {
					ls.Status = "idle"
					ls.Result = message
					ls.FEN = start.String()
					ls.Board = boardFromPosition(start)
				})
				time.Sleep(2 * time.Second)
				return
			}

			whiteDisplay := assignment.White.Name
			blackDisplay := assignment.Black.Name

			r.setLive(func(ls *LiveState) {
				ls.White = whiteDisplay
				ls.Black = blackDisplay
				ls.MovetimeMS = assignment.MovetimeMS
				ls.Status = "running"
				ls.Result = "*"
				ls.MovesUCI = nil
				ls.BookPlies = 0
			})
			r.b.Publish()

			whiteArgs := strings.Fields(assignment.White.Args)
			blackArgs := strings.Fields(assignment.Black.Args)

			white := NewUCIEngine(assignment.White.Path, whiteArgs)
			selfplay := assignment.White.ID == assignment.Black.ID
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
			movesUCI := make([]string, 0, 256)
			bookPlies := 0
			engineLogs := make([]db.EngineLog, 0, 256)

			bookMoves := r.bookLine(game.Position(), assignment)
			if len(bookMoves) > 0 {
				n := chess.UCINotation{}
				for _, move := range bookMoves {
					mv, err := n.Decode(game.Position(), move)
					if err != nil {
						log.Printf("runner: book move decode error: %v", err)
						break
					}
					if err := game.Move(mv); err != nil {
						log.Printf("runner: book move apply error: %v", err)
						break
					}
					movesUCI = append(movesUCI, move)
				}
				bookPlies = len(movesUCI)
			}

			r.setLive(func(ls *LiveState) {
				ls.FEN = game.Position().String()
				ls.Board = boardFromPosition(game.Position())
				ls.MovesUCI = append([]string(nil), movesUCI...)
				ls.BookPlies = bookPlies
			})
			r.b.Publish()

			for {
				select {
				case <-r.stop:
					r.failGame(ctx, "*", "service stopping")
					return
				default:
				}

				if len(movesUCI) >= 400 {
					result := "1/2-1/2"
					termination := "Max plies"
					gameID, err := r.store.InsertFinishedGame(ctx, assignment.White.ID, assignment.Black.ID, assignment.MovetimeMS, assignment.BookPath, result, termination, strings.Join(movesUCI, " "), bookPlies)
					if err != nil {
						log.Printf("runner: insert game error: %v", err)
					} else if err := r.store.InsertEngineLogs(ctx, gameID, engineLogs); err != nil {
						log.Printf("runner: insert engine logs error: %v", err)
					}
					r.setLive(func(ls *LiveState) {
						ls.Status = "finished"
						ls.Result = result
					})
					r.b.Publish()
					return
				}

				// Claim draws by 3-fold repetition or 50-move rule (instead of waiting for automatic
				// 5-fold or 75-move).
				if game.Outcome() == chess.NoOutcome {
					for _, method := range game.EligibleDraws() {
						if method == chess.ThreefoldRepetition || method == chess.FiftyMoveRule {
							_ = game.Draw(method)
							break
						}
					}
				}

				if game.Outcome() != chess.NoOutcome {
					result, termination := outcomeToResult(game)
					gameID, err := r.store.InsertFinishedGame(ctx, assignment.White.ID, assignment.Black.ID, assignment.MovetimeMS, assignment.BookPath, result, termination, strings.Join(movesUCI, " "), bookPlies)
					if err != nil {
						log.Printf("runner: insert game error: %v", err)
					} else if err := r.store.InsertEngineLogs(ctx, gameID, engineLogs); err != nil {
						log.Printf("runner: insert engine logs error: %v", err)
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

				ply := len(movesUCI) + 1
				moveTimeoutMS := assignment.MovetimeMS
				if moveTimeoutMS <= 0 {
					moveTimeoutMS = 100
				}
				if settings.GameSlackMS > 0 {
					moveTimeoutMS += settings.GameSlackMS
				}
				moveCtx, cancelMove := context.WithTimeout(ctx, time.Duration(moveTimeoutMS)*time.Millisecond)
				start := time.Now()
				best, logLines, err := eng.BestMoveMovetime(moveCtx, movesUCI, assignment.MovetimeMS)
				elapsedMS := time.Since(start).Milliseconds()
				cancelMove()
				engineID := assignment.White.ID
				if !isWhiteToMove {
					engineID = assignment.Black.ID
				}
				engineLogs = append(engineLogs, db.EngineLog{
					Ply:       ply,
					EngineID:  engineID,
					ElapsedMS: elapsedMS,
					Log:       strings.Join(logLines, "\n"),
				})
				if err != nil {
					if errors.Is(err, context.Canceled) {
						r.failGame(ctx, "*", "service stopping")
						return
					}
					termination := "EngineCrash"
					if errors.Is(err, context.DeadlineExceeded) {
						termination = "Timeout"
					}
					r.recordFailedGame(ctx, assignment, isWhiteToMove, movesUCI, bookPlies, termination, engineLogs)
					return
				}
				if best == "(none)" || best == "0000" {
					r.recordFailedGame(ctx, assignment, isWhiteToMove, movesUCI, bookPlies, "NoMove", engineLogs)
					return
				}

				n := chess.UCINotation{}
				mv, err := n.Decode(game.Position(), best)
				if err != nil {
					r.recordFailedGame(ctx, assignment, isWhiteToMove, movesUCI, bookPlies, "IllegalMove", engineLogs)
					return
				}

				if err := game.Move(mv); err != nil {
					r.recordFailedGame(ctx, assignment, isWhiteToMove, movesUCI, bookPlies, "IllegalMove", engineLogs)
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

func (r *Runner) recordFailedGame(ctx context.Context, assignment ColorAssignment, isWhiteToMove bool, movesUCI []string, bookPlies int, termination string, engineLogs []db.EngineLog) {
	result := "1-0"
	if isWhiteToMove {
		result = "0-1"
	}
	gameID, err := r.store.InsertFinishedGame(ctx, assignment.White.ID, assignment.Black.ID, assignment.MovetimeMS, assignment.BookPath, result, termination, strings.Join(movesUCI, " "), bookPlies)
	if err != nil {
		log.Printf("runner: insert game error: %v", err)
	} else if err := r.store.InsertEngineLogs(ctx, gameID, engineLogs); err != nil {
		log.Printf("runner: insert engine logs error: %v", err)
	}
	r.setLive(func(ls *LiveState) {
		ls.Status = "finished"
		ls.Result = result
	})
	r.b.Publish()
}

func (r *Runner) fillGameQueue(ctx context.Context, settings db.Settings) error {
	if r.store == nil {
		return nil
	}

	rows, err := r.store.ResultsByPair(ctx)
	if err != nil {
		return err
	}
	elos := ranking.ComputeBradleyTerryElos(rows, 3600)
	if err := r.store.ReplaceEngineElos(ctx, elos); err != nil {
		return err
	}
	engines, err := r.store.ListEngines(ctx)
	if err != nil {
		return err
	}
	weightedPairs := buildDistanceWeightedPairs(engines, settings.MatchSoftScale, settings.MatchAllowMirror)
	if len(weightedPairs) == 0 {
		return nil
	}
	counts, err := r.store.ListMatchupCounts(ctx)
	if err != nil {
		return err
	}

	type pairCount struct {
		AID      int64
		BID      int64
		AB       int
		BA       int
		Distance float64
		Weight   float64
	}

	pairCounts := make(map[[2]int64]*pairCount, len(weightedPairs))
	for _, pair := range weightedPairs {
		pairCounts[[2]int64{pair.AID, pair.BID}] = &pairCount{AID: pair.AID, BID: pair.BID, Distance: pair.Distance, Weight: pair.Weight}
	}
	for _, c := range counts {
		a := c.WhiteID
		b := c.BlackID
		key := [2]int64{a, b}
		if a > b {
			key = [2]int64{b, a}
			a, b = b, a
		}
		pc, ok := pairCounts[key]
		if !ok {
			continue
		}
		if c.WhiteID == pc.AID && c.BlackID == pc.BID {
			pc.AB += c.Count
		} else if c.WhiteID == pc.BID && c.BlackID == pc.AID {
			pc.BA += c.Count
		}
	}

	if len(pairCounts) == 0 {
		return nil
	}

	selected := make([]*pairCount, 0, len(pairCounts))
	for _, pc := range pairCounts {
		selected = append(selected, pc)
	}
	sort.Slice(selected, func(i, j int) bool {
		totalI := selected[i].AB + selected[i].BA
		totalJ := selected[j].AB + selected[j].BA
		scoreI := selected[i].Weight / float64(1+totalI)
		scoreJ := selected[j].Weight / float64(1+totalJ)
		if math.Abs(scoreI-scoreJ) > 1e-9 {
			return scoreI > scoreJ
		}
		if totalI != totalJ {
			return totalI < totalJ
		}
		if selected[i].Distance != selected[j].Distance {
			return selected[i].Distance < selected[j].Distance
		}
		if selected[i].AID != selected[j].AID {
			return selected[i].AID < selected[j].AID
		}
		return selected[i].BID < selected[j].BID
	})

	eligibleCount := len(eligibleEngines(engines))
	targetPairs := eligibleCount * 2
	if targetPairs < 4 {
		targetPairs = 4
	}
	if targetPairs > 32 {
		targetPairs = 32
	}
	if targetPairs > len(selected) {
		targetPairs = len(selected)
	}

	entries := make([]db.GameQueueEntry, 0, targetPairs*4)
	for _, pc := range selected[:targetPairs] {
		if pc.AID == pc.BID {
			for i := 0; i < 2; i++ {
				entries = append(entries, db.GameQueueEntry{
					WhiteID:    pc.AID,
					BlackID:    pc.BID,
					MovetimeMS: settings.GameMovetimeMS,
					BookPath:   settings.GameBookPath,
				})
			}
			continue
		}
		for i := 0; i < 2; i++ {
			entries = append(entries, db.GameQueueEntry{
				WhiteID:    pc.AID,
				BlackID:    pc.BID,
				MovetimeMS: settings.GameMovetimeMS,
				BookPath:   settings.GameBookPath,
			})
			entries = append(entries, db.GameQueueEntry{
				WhiteID:    pc.BID,
				BlackID:    pc.AID,
				MovetimeMS: settings.GameMovetimeMS,
				BookPath:   settings.GameBookPath,
			})
		}
	}

	return r.store.EnqueueGames(ctx, entries)
}
