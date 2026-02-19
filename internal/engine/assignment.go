package engine

import (
	"fmt"
	"strings"

	"tethys/internal/db"
)

type matchupCandidate struct {
	WhiteID int64
	BlackID int64
}

type ColorAssignment struct {
	White       db.Engine
	Black       db.Engine
	MovetimeMS  int
	BookEnabled bool
	BookPath    string
}

func selectAssignment(movetimeMS int, bookPath string, engines []db.Engine, matchups []db.Matchup, counts []db.MatchupCount, pickIdx int) (ColorAssignment, int) {
	assign := ColorAssignment{
		MovetimeMS:  movetimeMS,
		BookPath:    bookPath,
		BookEnabled: strings.TrimSpace(bookPath) != "",
	}
	if assign.MovetimeMS <= 0 {
		assign.MovetimeMS = 100
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
		if p.PlayerAID == 0 || p.PlayerBID == 0 {
			continue
		}
		if _, ok := engineByID[p.PlayerAID]; !ok {
			continue
		}
		if _, ok := engineByID[p.PlayerBID]; !ok {
			continue
		}
		validPairs = append(validPairs, p)
	}

	if len(validPairs) == 0 {
		return assign, 0
	}

	countMap := make(map[string]int)
	for _, c := range counts {
		key := fmt.Sprintf("%d\x00%d", c.WhiteID, c.BlackID)
		countMap[key] = c.Count
	}

	candidates := make([]matchupCandidate, 0, len(validPairs)*2)
	for _, p := range validPairs {
		if p.PlayerAID == p.PlayerBID {
			candidates = append(candidates, matchupCandidate{WhiteID: p.PlayerAID, BlackID: p.PlayerAID})
			continue
		}
		candidates = append(candidates, matchupCandidate{WhiteID: p.PlayerAID, BlackID: p.PlayerBID})
		candidates = append(candidates, matchupCandidate{WhiteID: p.PlayerBID, BlackID: p.PlayerAID})
	}

	minCount := -1
	for _, c := range candidates {
		key := fmt.Sprintf("%d\x00%d", c.WhiteID, c.BlackID)
		count := countMap[key]
		if minCount == -1 || count < minCount {
			minCount = count
		}
	}

	filtered := make([]matchupCandidate, 0, len(candidates))
	for _, c := range candidates {
		key := fmt.Sprintf("%d\x00%d", c.WhiteID, c.BlackID)
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
