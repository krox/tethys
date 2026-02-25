package engine

import (
	"strings"

	"tethys/internal/db"
)

type ColorAssignment struct {
	White       db.Engine
	Black       db.Engine
	MovetimeMS  int
	BookEnabled bool
	BookPath    string
}

type matchupPair struct {
	AID int64
	BID int64
}

func buildNeighborPairs(engines []db.Engine, radius int) []matchupPair {
	if radius < 1 {
		return nil
	}

	eligible := make([]db.Engine, 0, len(engines))
	for _, e := range engines {
		if e.ID == 0 || e.Name == "" || e.Path == "" {
			continue
		}
		eligible = append(eligible, e)
	}
	if len(eligible) < 2 {
		return nil
	}

	pairs := make(map[[2]int64]bool)
	for i, engine := range eligible {
		start := i - radius
		if start < 0 {
			start = 0
		}
		end := i + radius
		if end >= len(eligible) {
			end = len(eligible) - 1
		}
		for j := start; j <= end; j++ {
			if i == j {
				continue
			}
			other := eligible[j]
			a := engine.ID
			b := other.ID
			if a > b {
				a, b = b, a
			}
			pairs[[2]int64{a, b}] = true
		}
	}

	out := make([]matchupPair, 0, len(pairs))
	for key := range pairs {
		out = append(out, matchupPair{AID: key[0], BID: key[1]})
	}
	return out
}

func assignmentFromQueue(entry db.GameQueueEntry, enginesByID map[int64]db.Engine) (ColorAssignment, bool) {
	white, ok := enginesByID[entry.WhiteID]
	if !ok {
		return ColorAssignment{}, false
	}
	black, ok := enginesByID[entry.BlackID]
	if !ok {
		return ColorAssignment{}, false
	}
	assign := ColorAssignment{
		White:      white,
		Black:      black,
		MovetimeMS: entry.MovetimeMS,
		BookPath:   entry.BookPath,
	}
	if assign.MovetimeMS <= 0 {
		assign.MovetimeMS = 100
	}
	assign.BookEnabled = strings.TrimSpace(assign.BookPath) != ""
	return assign, true
}
