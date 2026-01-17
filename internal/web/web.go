package web

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"

	"tethys/internal/configstore"
	"tethys/internal/db"
	"tethys/internal/engine"
)

//go:embed templates/*.html
var templatesFS embed.FS

//go:embed static/*
var staticFS embed.FS

type Handler struct {
	store *db.Store
	conf  *configstore.Store
	r     *engine.Runner
	b     *engine.Broadcaster

	adminToken string

	tpl *template.Template
}

func NewHandler(store *db.Store, conf *configstore.Store, r *engine.Runner, b *engine.Broadcaster, adminToken string) *Handler {
	tpl := template.Must(template.New("base").ParseFS(templatesFS, "templates/*.html"))
	return &Handler{
		store:      store,
		conf:       conf,
		r:          r,
		b:          b,
		adminToken: adminToken,
		tpl:        tpl,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(staticSub)))

	mux.HandleFunc("GET /{$}", h.handleIndex)
	mux.HandleFunc("GET /live/fragment", h.handleLiveFragment)
	mux.Handle("GET /api/live/events", engine.SSEHandler(h.b))
	mux.HandleFunc("GET /api/live", h.handleLiveJSON)
	mux.HandleFunc("GET /opening", h.handleOpeningPage)
	mux.HandleFunc("GET /opening/fragment", h.handleOpeningFragment)
	mux.HandleFunc("GET /results", h.handleResults)

	mux.HandleFunc("GET /games", h.handleGames)
	mux.HandleFunc("GET /games/matchup.txt", h.handleMatchupMoves)
	mux.HandleFunc("GET /games/", h.handleGameMoves) // /games/{id}.txt
	mux.HandleFunc("POST /games/delete", h.requireAdmin(h.handleMatchupDelete))
	mux.HandleFunc("GET /download/all.txt", h.handleAllMoves)

	mux.HandleFunc("GET /admin", h.requireAdmin(h.handleAdminRoot))
	mux.HandleFunc("GET /admin/settings", h.requireAdmin(h.handleAdminSettings))
	mux.HandleFunc("POST /admin/settings", h.requireAdmin(h.handleAdminSettingsSave))
	mux.HandleFunc("GET /admin/matches", h.requireAdmin(h.handleAdminMatches))
	mux.HandleFunc("POST /admin/matches", h.requireAdmin(h.handleAdminMatchesSave))
	mux.HandleFunc("GET /admin/engines", h.requireAdmin(h.handleAdminEngines))
	mux.HandleFunc("POST /admin/engines", h.requireAdmin(h.handleAdminEnginesSave))
	mux.HandleFunc("POST /admin/restart", h.requireAdmin(h.handleAdminRestart))
	mux.HandleFunc("POST /admin/logout", h.requireAdmin(h.handleAdminLogout))
}
