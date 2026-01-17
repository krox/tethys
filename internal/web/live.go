package web

import (
	"encoding/json"
	"net/http"
)

func (h *Handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	_ = h.tpl.ExecuteTemplate(w, "index.html", map[string]any{})
}

func (h *Handler) handleLiveFragment(w http.ResponseWriter, r *http.Request) {
	live := h.r.Live()
	_ = h.tpl.ExecuteTemplate(w, "live_fragment.html", live)
}

func (h *Handler) handleLiveJSON(w http.ResponseWriter, r *http.Request) {
	live := h.r.Live()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"game_id":     live.GameID,
		"status":      live.Status,
		"white":       live.White,
		"black":       live.Black,
		"movetime_ms": live.MovetimeMS,
		"result":      live.Result,
		"fen":         live.FEN,
		"moves_uci":   live.MovesUCI,
	})
}
