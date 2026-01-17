package web

import (
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"

	"tethys/internal/config"
	"tethys/internal/configstore"
	"tethys/internal/db"
	"tethys/internal/engine"
)

//go:embed templates/*.html
var templatesFS embed.FS

//go:embed static/*
var staticFS embed.FS

type Handler struct {
	cfg   config.Config
	store *db.Store
	conf  *configstore.Store
	r     *engine.Runner
	b     *engine.Broadcaster

	tpl *template.Template

	sessionsMu sync.Mutex
	sessions   map[string]struct{}
}

func NewHandler(cfg config.Config, store *db.Store, conf *configstore.Store, r *engine.Runner, b *engine.Broadcaster) *Handler {
	tpl := template.Must(template.New("base").ParseFS(templatesFS, "templates/*.html"))
	return &Handler{
		cfg:      cfg,
		store:    store,
		conf:     conf,
		r:        r,
		b:        b,
		tpl:      tpl,
		sessions: make(map[string]struct{}),
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



	mux.HandleFunc("GET /games", h.handleGames)
	mux.HandleFunc("GET /games/", h.handleGameMoves) // /games/{id}.txt
	mux.HandleFunc("GET /download/all.txt", h.handleAllMoves)

	mux.HandleFunc("GET /admin", h.requireAdmin(h.handleAdmin))
	mux.HandleFunc("POST /admin/config", h.requireAdmin(h.handleAdminConfig))
	mux.HandleFunc("POST /admin/restart", h.requireAdmin(h.handleAdminRestart))
	mux.HandleFunc("GET /admin/login", h.handleAdminLogin)
	mux.HandleFunc("POST /admin/login", h.handleAdminLoginPost)
	mux.HandleFunc("POST /admin/logout", h.requireAdmin(h.handleAdminLogout))
}

func (h *Handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	_ = h.tpl.ExecuteTemplate(w, "index.html", map[string]any{})
}

func (h *Handler) handleOpeningPage(w http.ResponseWriter, r *http.Request) {
	_ = h.tpl.ExecuteTemplate(w, "opening.html", map[string]any{})
}

func (h *Handler) handleLiveFragment(w http.ResponseWriter, r *http.Request) {
	live := h.r.Live()
	_ = h.tpl.ExecuteTemplate(w, "live_fragment.html", live)
}

func (h *Handler) handleOpeningFragment(w http.ResponseWriter, r *http.Request) {
	const (
		maxPlies = 16
		maxGames = 2000
	)
	conf, err := h.conf.GetConfig(r.Context())
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

func (h *Handler) handleGames(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	games, err := h.store.ListFinishedGames(ctx, 200)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = h.tpl.ExecuteTemplate(w, "games.html", map[string]any{"Games": games})
}

func (h *Handler) handleGameMoves(w http.ResponseWriter, r *http.Request) {
	base := path.Base(r.URL.Path)
	if !strings.HasSuffix(base, ".txt") {
		http.NotFound(w, r)
		return
	}
	idStr := strings.TrimSuffix(base, ".txt")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	moves, result, err := h.store.GameMoves(r.Context(), id)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	line := moves
	if line != "" {
		line += " "
	}
	line += result
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=game-%d.txt", id))
	_, _ = w.Write([]byte(line + "\n"))
}

func (h *Handler) handleAllMoves(w http.ResponseWriter, r *http.Request) {
	lines, err := h.store.AllFinishedMovesLines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=tethys-all.txt")
	_, _ = w.Write([]byte(lines))
}

func (h *Handler) handleAdminLogin(w http.ResponseWriter, r *http.Request) {
	if h.cfg.AdminPassword == "" {
		http.Error(w, "/admin disabled (no TETHYS_ADMIN_PASSWORD)", http.StatusForbidden)
		return
	}
	_ = h.tpl.ExecuteTemplate(w, "admin_login.html", map[string]any{"Error": ""})
}

func (h *Handler) handleAdminLoginPost(w http.ResponseWriter, r *http.Request) {
	if h.cfg.AdminPassword == "" {
		http.Error(w, "/admin disabled (no TETHYS_ADMIN_PASSWORD)", http.StatusForbidden)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	pw := r.Form.Get("password")
	if pw != h.cfg.AdminPassword {
		_ = h.tpl.ExecuteTemplate(w, "admin_login.html", map[string]any{"Error": "wrong password"})
		return
	}
	ok := h.newSession()
	http.SetCookie(w, &http.Cookie{Name: "tethys_admin", Value: ok, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
	http.Redirect(w, r, "/admin", http.StatusSeeOther)
}

func (h *Handler) handleAdminLogout(w http.ResponseWriter, r *http.Request) {
	cookie, _ := r.Cookie("tethys_admin")
	if cookie != nil {
		h.sessionsMu.Lock()
		delete(h.sessions, cookie.Value)
		h.sessionsMu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: "tethys_admin", Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (h *Handler) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.cfg.AdminPassword == "" {
			http.Error(w, "/admin disabled (no TETHYS_ADMIN_PASSWORD)", http.StatusForbidden)
			return
		}
		cookie, err := r.Cookie("tethys_admin")
		if err != nil || cookie.Value == "" {
			http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
			return
		}
		h.sessionsMu.Lock()
		_, ok := h.sessions[cookie.Value]
		h.sessionsMu.Unlock()
		if !ok {
			http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
			return
		}
		next(w, r)
	}
}

func (h *Handler) handleAdmin(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.conf.GetConfig(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = h.tpl.ExecuteTemplate(w, "admin.html", map[string]any{"Cfg": cfg})
}

func (h *Handler) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	movetime, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("movetime_ms")))
	if movetime <= 0 {
		movetime = 100
	}
	maxPlies, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("max_plies")))
	if maxPlies <= 0 {
		maxPlies = 200
	}
	openingMin, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("opening_min")))
	if openingMin <= 0 {
		openingMin = 20
	}
	bookMaxPlies, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("book_max_plies")))
	if bookMaxPlies <= 0 {
		bookMaxPlies = 16
	}
	bookEnabled := r.Form.Get("book_enabled") == "on"
	selfplay := r.Form.Get("selfplay") == "on"
	cfg := configstore.Config{
		EngineA: configstore.EngineConfig{
			Path: r.Form.Get("engine_a_path"),
			Args: r.Form.Get("engine_a_args"),
			Init: r.Form.Get("engine_a_init"),
		},
		EngineB: configstore.EngineConfig{
			Path: r.Form.Get("engine_b_path"),
			Args: r.Form.Get("engine_b_args"),
			Init: r.Form.Get("engine_b_init"),
		},
		MovetimeMS: movetime,
		Selfplay:   selfplay,
		MaxPlies:   maxPlies,
		OpeningMin: openingMin,
		BookEnabled:  bookEnabled,
		BookPath:     strings.TrimSpace(r.Form.Get("book_path")),
		BookMaxPlies: bookMaxPlies,
	}
	if err := h.conf.UpdateConfig(r.Context(), cfg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin", http.StatusSeeOther)
}

func (h *Handler) handleAdminRestart(w http.ResponseWriter, r *http.Request) {
	h.r.Restart()
	http.Redirect(w, r, "/admin", http.StatusSeeOther)
}

func (h *Handler) newSession() string {
	buf := make([]byte, 32)
	_, _ = rand.Read(buf)
	id := hex.EncodeToString(buf)
	h.sessionsMu.Lock()
	h.sessions[id] = struct{}{}
	h.sessionsMu.Unlock()
	return id
}
