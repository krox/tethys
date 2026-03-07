package web

import (
	"net/http"
	"sort"

	"tethys/internal/db"
	"tethys/internal/ranking"
)

type RankingRow struct {
	Rank  int
	Name  string
	Elo   float64
	Games int
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
	engines, err := h.store.ListEngines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rows, err := h.store.ResultsByPair(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(engines) == 0 && len(rows) > 0 {
		byID := make(map[int64]db.Engine)
		for _, row := range rows {
			if row.EngineAID != 0 {
				byID[row.EngineAID] = db.Engine{ID: row.EngineAID, Name: row.EngineA}
			}
			if row.EngineBID != 0 {
				byID[row.EngineBID] = db.Engine{ID: row.EngineBID, Name: row.EngineB}
			}
		}
		engines = make([]db.Engine, 0, len(byID))
		for _, engine := range byID {
			engines = append(engines, engine)
		}
		sort.Slice(engines, func(i, j int) bool {
			return engines[i].Name < engines[j].Name
		})
	}
	matchupsByEngine := buildMatchupsByEngine(rows)
	gamesByEngine := buildGamesByEngine(rows)
	eloByName := make(map[string]float64, len(engines))
	for _, eng := range engines {
		eloByName[eng.Name] = eng.Elo
	}
	view := make([]RankingView, 0, len(engines))
	for i, eng := range engines {
		matchups := matchupsByEngine[eng.Name]
		sort.Slice(matchups, func(i, j int) bool {
			eloI := eloByName[matchups[i].Opponent]
			eloJ := eloByName[matchups[j].Opponent]
			if eloI == eloJ {
				return matchups[i].Opponent < matchups[j].Opponent
			}
			return eloI > eloJ
		})
		view = append(view, RankingView{RankingRow: RankingRow{
			Rank:  i + 1,
			Name:  eng.Name,
			Elo:   eng.Elo,
			Games: gamesByEngine[eng.Name],
		}, Matchups: matchups})
	}
	_ = h.tpl.ExecuteTemplate(w, "ranking.html", map[string]any{
		"Rankings": view,
		"Page":     "ranking",
	})
}

func (h *Handler) handleRankingRecompute(w http.ResponseWriter, r *http.Request) {
	rows, err := h.store.ResultsByPair(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	elos := ranking.ComputeBradleyTerryElos(rows, 3600)
	if err := h.store.ReplaceEngineElos(r.Context(), elos); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/results", http.StatusSeeOther)
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

func buildGamesByEngine(rows []db.PairResult) map[string]int {
	games := make(map[string]int)
	for _, row := range rows {
		total := row.WinsA + row.WinsB + row.Draws
		if total == 0 {
			continue
		}
		games[row.EngineA] += total
		if row.EngineA != row.EngineB {
			games[row.EngineB] += total
		}
	}
	return games
}
