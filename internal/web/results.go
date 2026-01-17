package web

import (
	"math"
	"net/http"
	"sort"

	"tethys/internal/db"
)

type RankingRow struct {
	Rank        int
	Name        string
	Strength    float64
	ScorePct    float64
	Games       int
	StrengthPct float64
}

type MatchupBreakdown struct {
	Opponent string
	Wins     int
	Losses   int
	Draws    int
	Total    int
	WinPct   float64
	LossPct  float64
	DrawPct  float64
}

type RankingView struct {
	RankingRow
	Matchups []MatchupBreakdown
}

func (h *Handler) handleResults(w http.ResponseWriter, r *http.Request) {
	rows, err := h.store.ResultsByPair(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rankings := computeBradleyTerry(rows)
	matchupsByEngine := buildMatchupsByEngine(rows)
	view := make([]RankingView, 0, len(rankings))
	for _, r := range rankings {
		matchups := matchupsByEngine[r.Name]
		sort.Slice(matchups, func(i, j int) bool {
			if matchups[i].Total == matchups[j].Total {
				return matchups[i].Opponent < matchups[j].Opponent
			}
			return matchups[i].Total > matchups[j].Total
		})
		view = append(view, RankingView{RankingRow: r, Matchups: matchups})
	}
	_ = h.tpl.ExecuteTemplate(w, "ranking.html", map[string]any{
		"Rankings": view,
		"IsAdmin":  h.isAdminRequest(w, r),
		"Page":     "ranking",
	})
}

func buildMatchupsByEngine(rows []db.PairResult) map[string][]MatchupBreakdown {
	matchups := make(map[string][]MatchupBreakdown)
	for _, row := range rows {
		total := row.WinsA + row.WinsB + row.Draws
		if total == 0 {
			continue
		}
		matchups[row.EngineA] = append(matchups[row.EngineA], MatchupBreakdown{
			Opponent: row.EngineB,
			Wins:     row.WinsA,
			Losses:   row.WinsB,
			Draws:    row.Draws,
			Total:    total,
			WinPct:   float64(row.WinsA) * 100 / float64(total),
			LossPct:  float64(row.WinsB) * 100 / float64(total),
			DrawPct:  float64(row.Draws) * 100 / float64(total),
		})
		if row.EngineA == row.EngineB {
			continue
		}
		matchups[row.EngineB] = append(matchups[row.EngineB], MatchupBreakdown{
			Opponent: row.EngineA,
			Wins:     row.WinsB,
			Losses:   row.WinsA,
			Draws:    row.Draws,
			Total:    total,
			WinPct:   float64(row.WinsB) * 100 / float64(total),
			LossPct:  float64(row.WinsA) * 100 / float64(total),
			DrawPct:  float64(row.Draws) * 100 / float64(total),
		})
	}
	return matchups
}

func computeBradleyTerry(rows []db.PairResult) []RankingRow {
	index := make(map[string]int)
	for _, row := range rows {
		if _, ok := index[row.EngineA]; !ok {
			index[row.EngineA] = len(index)
		}
		if _, ok := index[row.EngineB]; !ok {
			index[row.EngineB] = len(index)
		}
	}
	if len(index) == 0 {
		return nil
	}
	engineNames := make([]string, len(index))
	for name, idx := range index {
		engineNames[idx] = name
	}

	n := len(index)
	games := make([][]float64, n)
	wins := make([][]float64, n)
	for i := 0; i < n; i++ {
		games[i] = make([]float64, n)
		wins[i] = make([]float64, n)
	}
	for _, row := range rows {
		i := index[row.EngineA]
		j := index[row.EngineB]
		if i == j {
			continue
		}
		wA := float64(row.WinsA) + 0.5*float64(row.Draws)
		wB := float64(row.WinsB) + 0.5*float64(row.Draws)
		nij := float64(row.WinsA + row.WinsB + row.Draws)
		games[i][j] += nij
		games[j][i] += nij
		wins[i][j] += wA
		wins[j][i] += wB
	}

	strength := make([]float64, n)
	for i := range strength {
		strength[i] = 1.0
	}
	for iter := 0; iter < 200; iter++ {
		maxDelta := 0.0
		for i := 0; i < n; i++ {
			wi := 0.0
			for j := 0; j < n; j++ {
				wi += wins[i][j]
			}
			if wi == 0 {
				strength[i] = 0.0
				continue
			}
			denom := 0.0
			for j := 0; j < n; j++ {
				if i == j {
					continue
				}
				if games[i][j] == 0 {
					continue
				}
				sum := strength[i] + strength[j]
				if sum <= 0 {
					sum = 1
				}
				denom += games[i][j] / sum
			}
			if denom == 0 {
				continue
			}
			newStrength := wi / denom
			delta := math.Abs(newStrength - strength[i])
			if delta > maxDelta {
				maxDelta = delta
			}
			strength[i] = newStrength
		}
		if maxDelta < 1e-6 {
			break
		}
	}

	maxStrength := 0.0
	for _, s := range strength {
		if s > maxStrength {
			maxStrength = s
		}
	}
	if maxStrength == 0 {
		maxStrength = 1
	}

	result := make([]RankingRow, 0, n)
	for i := 0; i < n; i++ {
		totalGames := 0.0
		winScore := 0.0
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			totalGames += games[i][j]
			winScore += wins[i][j]
		}
		if totalGames == 0 {
			continue
		}
		result = append(result, RankingRow{
			Name:        engineNames[i],
			Strength:    strength[i],
			ScorePct:    winScore * 100 / totalGames,
			Games:       int(totalGames),
			StrengthPct: strength[i] * 100 / maxStrength,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Strength == result[j].Strength {
			return result[i].Name < result[j].Name
		}
		return result[i].Strength > result[j].Strength
	})
	for i := range result {
		result[i].Rank = i + 1
	}
	return result
}
