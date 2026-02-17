package web

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	gameCount, err := h.store.CountGames(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	engineCount, err := h.store.CountEngines(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	recentGames, err := h.store.ListFinishedGames(ctx, 10)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = h.tpl.ExecuteTemplate(w, "live_view.html", map[string]any{
		"Page":        "live",
		"GameCount":   gameCount,
		"EngineCount": engineCount,
		"RecentGames": recentGames,
	})
}

func (h *Handler) handleLiveFragment(w http.ResponseWriter, r *http.Request) {
	live := h.r.Live()
	_ = h.tpl.ExecuteTemplate(w, "live_fragment.html", live)
}

func (h *Handler) handleLiveJSON(w http.ResponseWriter, r *http.Request) {
	live := h.r.Live()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":      live.Status,
		"white":       live.White,
		"black":       live.Black,
		"movetime_ms": live.MovetimeMS,
		"result":      live.Result,
		"fen":         live.FEN,
		"moves_uci":   live.MovesUCI,
	})
}
