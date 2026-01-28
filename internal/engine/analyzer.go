package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/notnil/chess"

	"tethys/internal/book"
	"tethys/internal/configstore"
	"tethys/internal/db"
)

type AnalysisInfo struct {
	ZobristKey uint64
	FEN        string
	Score      string
	PV         string
	EngineID   int64
	Depth      int
	UpdatedAt  time.Time
	Done       bool
	Err        string
}

type Analyzer struct {
	store *db.Store
	conf  *configstore.Store

	mu     sync.Mutex
	jobs   map[uint64]context.CancelFunc
	latest map[uint64]AnalysisInfo
}

func NewAnalyzer(store *db.Store, conf *configstore.Store) *Analyzer {
	return &Analyzer{
		store:  store,
		conf:   conf,
		jobs:   make(map[uint64]context.CancelFunc),
		latest: make(map[uint64]AnalysisInfo),
	}
}

func (a *Analyzer) Latest(key uint64) (AnalysisInfo, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	info, ok := a.latest[key]
	return info, ok
}

func (a *Analyzer) EnsureAnalysis(ctx context.Context, fen string) (AnalysisInfo, error) {
	fenKey, fullFen, err := normalizeFEN(fen)
	if err != nil {
		return AnalysisInfo{}, err
	}
	key, err := zobristFromFEN(fullFen)
	if err != nil {
		return AnalysisInfo{}, err
	}

	info := AnalysisInfo{ZobristKey: key, FEN: fenKey}
	if a.store != nil {
		if cached, err := a.store.EvalByZobrist(ctx, key); err == nil {
			info.Score = cached.Score
			info.PV = cached.PV
			info.EngineID = cached.EngineID
			info.Depth = cached.Depth
		}
	}

	a.mu.Lock()
	if latest, ok := a.latest[key]; ok {
		info = mergeAnalysis(info, latest)
	}
	if _, running := a.jobs[key]; !running {
		jobCtx, cancel := context.WithCancel(context.Background())
		a.jobs[key] = cancel
		go a.run(jobCtx, key, fenKey, fullFen)
	}
	a.latest[key] = info
	a.mu.Unlock()

	return info, nil
}

func (a *Analyzer) run(ctx context.Context, key uint64, fenKey string, fullFen string) {
	defer func() {
		a.mu.Lock()
		delete(a.jobs, key)
		a.mu.Unlock()
	}()

	cfg, err := a.conf.GetConfig(ctx)
	if err != nil {
		a.updateError(key, fenKey, fmt.Sprintf("config error: %v", err))
		return
	}
	engineID := cfg.AnalysisEngineID
	depth := cfg.AnalysisDepth
	if engineID <= 0 || depth <= 0 {
		a.updateError(key, fenKey, "analysis engine not configured")
		return
	}
	engRow, err := a.store.EngineByID(ctx, engineID)
	if err != nil {
		a.updateError(key, fenKey, "analysis engine missing")
		return
	}
	eng := NewUCIEngine(engRow.Path, strings.Fields(engRow.Args))
	if err := eng.Start(ctx); err != nil {
		a.updateError(key, fenKey, fmt.Sprintf("engine start error: %v", err))
		return
	}
	defer func() { _ = eng.Close() }()
	if err := applyInit(ctx, eng, engRow.Init); err != nil {
		a.updateError(key, fenKey, fmt.Sprintf("engine init error: %v", err))
		return
	}
	if err := eng.Send("position fen " + fullFen); err != nil {
		a.updateError(key, fenKey, fmt.Sprintf("position error: %v", err))
		return
	}
	if err := eng.Send(fmt.Sprintf("go depth %d", depth)); err != nil {
		a.updateError(key, fenKey, fmt.Sprintf("go error: %v", err))
		return
	}

	latestDepth := 0
	for {
		line, err := eng.ReadLine()
		if err != nil {
			a.updateError(key, fenKey, fmt.Sprintf("engine read error: %v", err))
			return
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "bestmove ") {
			a.updateDone(key)
			return
		}
		depthVal, score, pv, ok := parseInfoLine(line)
		if !ok {
			continue
		}
		if depthVal < latestDepth {
			continue
		}
		latestDepth = depthVal
		update := AnalysisInfo{
			ZobristKey: key,
			FEN:        fenKey,
			Score:      score,
			PV:         pv,
			EngineID:   engineID,
			Depth:      depthVal,
			UpdatedAt:  time.Now(),
		}
		a.updateLatest(update)
	}
}

func (a *Analyzer) updateLatest(update AnalysisInfo) {
	a.mu.Lock()
	curr := a.latest[update.ZobristKey]
	update.Err = curr.Err
	a.latest[update.ZobristKey] = update
	a.mu.Unlock()

	if a.store != nil {
		_ = a.store.UpsertEval(context.Background(), db.Eval{
			ZobristKey: update.ZobristKey,
			FEN:        update.FEN,
			Score:      update.Score,
			PV:         update.PV,
			EngineID:   update.EngineID,
			Depth:      update.Depth,
		})
	}
}

func (a *Analyzer) updateError(key uint64, fenKey string, msg string) {
	a.mu.Lock()
	curr := a.latest[key]
	curr.ZobristKey = key
	curr.FEN = fenKey
	curr.Err = msg
	curr.UpdatedAt = time.Now()
	a.latest[key] = curr
	a.mu.Unlock()
}

func (a *Analyzer) updateDone(key uint64) {
	a.mu.Lock()
	curr := a.latest[key]
	curr.Done = true
	curr.UpdatedAt = time.Now()
	a.latest[key] = curr
	a.mu.Unlock()
}

func mergeAnalysis(base AnalysisInfo, other AnalysisInfo) AnalysisInfo {
	if other.Score != "" {
		base.Score = other.Score
	}
	if other.PV != "" {
		base.PV = other.PV
	}
	if other.EngineID != 0 {
		base.EngineID = other.EngineID
	}
	if other.Depth != 0 {
		base.Depth = other.Depth
	}
	if !other.UpdatedAt.IsZero() {
		base.UpdatedAt = other.UpdatedAt
	}
	base.Done = other.Done
	if other.Err != "" {
		base.Err = other.Err
	}
	return base
}

func normalizeFEN(fen string) (string, string, error) {
	parts := strings.Fields(strings.TrimSpace(fen))
	if len(parts) < 4 {
		return "", "", fmt.Errorf("invalid FEN")
	}
	fenKey := strings.Join(parts[:4], " ")
	full := fenKey + " 0 1"
	if _, err := chess.FEN(full); err != nil {
		return "", "", fmt.Errorf("invalid FEN")
	}
	return fenKey, full, nil
}

func zobristFromFEN(fullFen string) (uint64, error) {
	opt, err := chess.FEN(fullFen)
	if err != nil {
		return 0, err
	}
	game := chess.NewGame(opt)
	pos := game.Position()
	return book.ZobristKey(pos), nil
}

func parseInfoLine(line string) (int, string, string, bool) {
	if !strings.HasPrefix(line, "info ") {
		return 0, "", "", false
	}
	parts := strings.Fields(line)
	depth := 0
	score := ""
	pv := ""
	for i := 0; i < len(parts); i++ {
		switch parts[i] {
		case "depth":
			if i+1 < len(parts) {
				if v, err := strconv.Atoi(parts[i+1]); err == nil {
					depth = v
				}
				i++
			}
		case "score":
			if i+2 < len(parts) {
				score = parts[i+1] + " " + parts[i+2]
				i += 2
			}
		case "pv":
			if i+1 < len(parts) {
				pv = strings.Join(parts[i+1:], " ")
				i = len(parts)
			}
		}
	}
	if depth == 0 || score == "" {
		return 0, "", "", false
	}
	return depth, score, pv, true
}
