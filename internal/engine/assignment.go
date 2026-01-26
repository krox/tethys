package engine

import (
	"fmt"

	"tethys/internal/configstore"
	"tethys/internal/db"
)

type matchupCandidate struct {
	WhiteID   int64
	BlackID   int64
	RulesetID int64
}

type ColorAssignment struct {
	White        db.Engine
	Black        db.Engine
	MovetimeMS   int
	BookEnabled  bool
	BookPath     string
	BookMaxPlies int
	RulesetID    int64
}

func selectAssignment(cfg configstore.Config, engines []db.Engine, matchups []db.Matchup, rulesets map[int64]db.Ruleset, counts []db.MatchupCount, pickIdx int) (ColorAssignment, int) {
	assign := ColorAssignment{
		MovetimeMS:   cfg.MovetimeMS,
		BookEnabled:  cfg.BookEnabled,
		BookPath:     cfg.BookPath,
		BookMaxPlies: cfg.BookMaxPlies,
	}
	if assign.MovetimeMS <= 0 {
		assign.MovetimeMS = 100
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
		if p.PlayerAID == 0 || p.PlayerBID == 0 || p.RulesetID == 0 {
			continue
		}
		if _, ok := engineByID[p.PlayerAID]; !ok {
			continue
		}
		if _, ok := engineByID[p.PlayerBID]; !ok {
			continue
		}
		if _, ok := rulesets[p.RulesetID]; !ok {
			continue
		}
		validPairs = append(validPairs, p)
	}

	if len(validPairs) == 0 {
		return assign, 0
	}

	countMap := make(map[string]int)
	for _, c := range counts {
		key := fmt.Sprintf("%d\x00%d\x00%d", c.WhiteID, c.BlackID, c.RulesetID)
		countMap[key] = c.Count
	}

	candidates := make([]matchupCandidate, 0, len(validPairs)*2)
	for _, p := range validPairs {
		if p.PlayerAID == p.PlayerBID {
			candidates = append(candidates, matchupCandidate{WhiteID: p.PlayerAID, BlackID: p.PlayerAID, RulesetID: p.RulesetID})
			continue
		}
		candidates = append(candidates, matchupCandidate{WhiteID: p.PlayerAID, BlackID: p.PlayerBID, RulesetID: p.RulesetID})
		candidates = append(candidates, matchupCandidate{WhiteID: p.PlayerBID, BlackID: p.PlayerAID, RulesetID: p.RulesetID})
	}

	minCount := -1
	for _, c := range candidates {
		key := fmt.Sprintf("%d\x00%d\x00%d", c.WhiteID, c.BlackID, c.RulesetID)
		count := countMap[key]
		if minCount == -1 || count < minCount {
			minCount = count
		}
	}

	filtered := make([]matchupCandidate, 0, len(candidates))
	for _, c := range candidates {
		key := fmt.Sprintf("%d\x00%d\x00%d", c.WhiteID, c.BlackID, c.RulesetID)
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

	assign.RulesetID = chosen.RulesetID
	chosenRuleset := rulesets[chosen.RulesetID]
	if chosenRuleset.ID != 0 {
		assign.MovetimeMS = chosenRuleset.MovetimeMS
		assign.BookPath = chosenRuleset.BookPath
		assign.BookMaxPlies = chosenRuleset.BookMaxPlies
		assign.BookEnabled = chosenRuleset.BookPath != ""
	}

	nextIdx := (idx + 1) % len(filtered)
	return assign, nextIdx
}
