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
	store      *db.Store
	conf       *configstore.Store
	r          *engine.Runner
	b          *engine.Broadcaster
	an         *engine.Analyzer
	enginesDir string
	booksDir   string

	tpl *template.Template
}

func NewHandler(store *db.Store, conf *configstore.Store, r *engine.Runner, b *engine.Broadcaster, an *engine.Analyzer, enginesDir string, booksDir string) *Handler {
	tpl := template.Must(template.New("base").ParseFS(templatesFS, "templates/*.html"))
	return &Handler{
		store:      store,
		conf:       conf,
		r:          r,
		b:          b,
		an:         an,
		enginesDir: enginesDir,
		booksDir:   booksDir,
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
	mux.HandleFunc("GET /book", h.handleBookExplorer)
	mux.HandleFunc("GET /results", h.handleResults)
	mux.HandleFunc("POST /results/recompute", h.handleRankingRecompute)
	mux.HandleFunc("GET /positions/view", h.handlePositionView)
	mux.HandleFunc("GET /api/positions/eval", h.handlePositionEval)
	mux.HandleFunc("GET /api/positions/move", h.handlePositionMove)

	mux.HandleFunc("GET /games", h.handleGames)
	mux.HandleFunc("GET /games/matchup.txt", h.handleMatchupMoves)
	mux.HandleFunc("GET /games/result.txt", h.handleResultDownload)
	mux.HandleFunc("GET /games/", h.handleGameMoves) // /games/{id}.txt
	mux.HandleFunc("GET /games/view", h.handleGameView)
	mux.HandleFunc("POST /games/delete", h.handleMatchupDelete)
	mux.HandleFunc("POST /games/delete-result", h.handleResultDelete)

	mux.HandleFunc("GET /admin", h.handleAdminRoot)
	mux.HandleFunc("GET /admin/settings", h.handleAdminSettings)
	mux.HandleFunc("POST /admin/settings", h.handleAdminSettingsSave)
	mux.HandleFunc("GET /admin/matches", h.handleAdminMatches)
	mux.HandleFunc("POST /admin/matches", h.handleAdminMatchesSave)
	mux.HandleFunc("POST /admin/rulesets/add", h.handleAdminRulesetAdd)
	mux.HandleFunc("POST /admin/rulesets/delete", h.handleAdminRulesetDelete)
	mux.HandleFunc("GET /admin/engines", h.handleAdminEngines)
	mux.HandleFunc("POST /admin/engines", h.handleAdminEnginesSave)
	mux.HandleFunc("POST /admin/engines/duplicate", h.handleAdminEngineDuplicate)
	mux.HandleFunc("POST /admin/engines/add-external", h.handleAdminEngineAddExternal)
	mux.HandleFunc("POST /admin/engines/prune", h.handleAdminEnginePrune)
	mux.HandleFunc("POST /admin/logout", h.handleAdminLogout)
}
