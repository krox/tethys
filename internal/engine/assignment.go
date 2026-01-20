package engine

import (
	"fmt"

	"tethys/internal/configstore"
	"tethys/internal/db"
)

type matchupCandidate struct {
	WhiteID int64
	BlackID int64
}

type ColorAssignment struct {
	White        db.Engine
	Black        db.Engine
	MovetimeMS   int
	MaxPlies     int
	BookEnabled  bool
	BookPath     string
	BookMaxPlies int
}

func selectAssignment(cfg configstore.Config, engines []db.Engine, matchups []db.Matchup, counts []db.MatchupCount, pickIdx int) (ColorAssignment, int) {
	assign := ColorAssignment{
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

	engineByID := make(map[int64]db.Engine)
	for _, e := range engines {
		if e.ID == 0 || e.Name == "" || e.Path == "" {
			continue
		}
		engineByID[e.ID] = e
	}

	validPairs := make([]db.Matchup, 0, len(matchups))
	for _, p := range matchups {
		if p.EngineAID == 0 || p.EngineBID == 0 {
			continue
		}
		if _, ok := engineByID[p.EngineAID]; !ok {
			continue
		}
		if _, ok := engineByID[p.EngineBID]; !ok {
			continue
		}
		validPairs = append(validPairs, p)
	}

	if len(validPairs) == 0 {
		return assign, 0
	}

	countMap := make(map[string]int)
	for _, c := range counts {
		key := fmt.Sprintf("%d\x00%d\x00%d", c.WhiteID, c.BlackID, c.MovetimeMS)
		countMap[key] = c.Count
	}

	candidates := make([]matchupCandidate, 0, len(validPairs)*2)
	for _, p := range validPairs {
		if p.EngineAID == p.EngineBID {
			candidates = append(candidates, matchupCandidate{WhiteID: p.EngineAID, BlackID: p.EngineAID})
			continue
		}
		candidates = append(candidates, matchupCandidate{WhiteID: p.EngineAID, BlackID: p.EngineBID})
		candidates = append(candidates, matchupCandidate{WhiteID: p.EngineBID, BlackID: p.EngineAID})
	}

	minCount := -1
	for _, c := range candidates {
		key := fmt.Sprintf("%d\x00%d\x00%d", c.WhiteID, c.BlackID, assign.MovetimeMS)
		count := countMap[key]
		if minCount == -1 || count < minCount {
			minCount = count
		}
	}

	filtered := make([]matchupCandidate, 0, len(candidates))
	for _, c := range candidates {
		key := fmt.Sprintf("%d\x00%d\x00%d", c.WhiteID, c.BlackID, assign.MovetimeMS)
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

	white := engineByID[chosen.WhiteID]
	black := engineByID[chosen.BlackID]
	assign.White, assign.Black = white, black

	nextIdx := (idx + 1) % len(filtered)
	return assign, nextIdx
}
