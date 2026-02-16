package web

import "net/http"

func (h *Handler) handleOpeningPage(w http.ResponseWriter, r *http.Request) {
	_ = h.tpl.ExecuteTemplate(w, "opening_explorer.html", map[string]any{
		"Page": "opening",
	})
}

func (h *Handler) handleOpeningFragment(w http.ResponseWriter, r *http.Request) {
	const (
		maxPlies = 16
		maxGames = 2000
	)
	conf, err := h.store.GetSettings(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	opening, err := buildOpeningTree(r.Context(), h.store, maxPlies, maxGames, conf.OpeningMin)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = h.tpl.ExecuteTemplate(w, "opening_fragment.html", opening)
}
