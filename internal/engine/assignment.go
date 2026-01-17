package engine

import (
	"fmt"

	"tethys/internal/configstore"
	"tethys/internal/db"
)

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
