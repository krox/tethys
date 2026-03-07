package engine

import (
	"math"
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

type weightedMatchupPair struct {
	AID      int64
	BID      int64
	Distance float64
	Weight   float64
}

func eligibleEngines(engines []db.Engine) []db.Engine {
	eligible := make([]db.Engine, 0, len(engines))
	for _, e := range engines {
		if e.ID == 0 || e.Name == "" || e.Path == "" {
			continue
		}
		eligible = append(eligible, e)
	}
	return eligible
}

func buildDistanceWeightedPairs(engines []db.Engine, softScale int, allowMirror bool) []weightedMatchupPair {
	eligible := eligibleEngines(engines)
	if len(eligible) == 0 {
		return nil
	}
	if !allowMirror && len(eligible) < 2 {
		return nil
	}
	if softScale <= 0 {
		softScale = 300
	}

	estimated := len(eligible) * (len(eligible) - 1) / 2
	if allowMirror {
		estimated += len(eligible)
	}
	pairs := make([]weightedMatchupPair, 0, estimated)
	for i := 0; i < len(eligible); i++ {
		start := i + 1
		if allowMirror {
			start = i
		}
		for j := start; j < len(eligible); j++ {
			a := eligible[i]
			b := eligible[j]
			distance := math.Abs(a.Elo - b.Elo)
			ratio := distance / float64(softScale)
			weight := math.Exp(-(ratio * ratio))
			pairs = append(pairs, weightedMatchupPair{
				AID:      a.ID,
				BID:      b.ID,
				Distance: distance,
				Weight:   weight,
			})
		}
	}
	return pairs
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
