package web

import (
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"math"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	mux.HandleFunc("GET /results", h.handleResults)

	mux.HandleFunc("GET /games", h.handleGames)
	mux.HandleFunc("GET /games/", h.handleGameMoves) // /games/{id}.txt
	mux.HandleFunc("GET /download/all.txt", h.handleAllMoves)

	mux.HandleFunc("GET /admin", h.requireAdmin(h.handleAdminRoot))
	mux.HandleFunc("GET /admin/settings", h.requireAdmin(h.handleAdminSettings))
	mux.HandleFunc("POST /admin/settings", h.requireAdmin(h.handleAdminSettingsSave))
	mux.HandleFunc("GET /admin/matches", h.requireAdmin(h.handleAdminMatches))
	mux.HandleFunc("POST /admin/matches", h.requireAdmin(h.handleAdminMatchesSave))
	mux.HandleFunc("GET /admin/engines", h.requireAdmin(h.handleAdminEngines))
	mux.HandleFunc("POST /admin/engines", h.requireAdmin(h.handleAdminEnginesSave))
	mux.HandleFunc("POST /admin/restart", h.requireAdmin(h.handleAdminRestart))
	mux.HandleFunc("GET /admin/login", h.handleAdminLogin)
	mux.HandleFunc("POST /admin/login", h.handleAdminLoginPost)
	mux.HandleFunc("POST /admin/logout", h.requireAdmin(h.handleAdminLogout))
}

type ResultsRow struct {
	EngineA string
	EngineB string
	Wins    int
	Losses  int
	Draws   int
	Total   int
	WinPct  float64
	LossPct float64
	DrawPct float64
}

type RankingRow struct {
	Rank       int
	Name       string
	Strength   float64
	ScorePct   float64
	Games      int
	StrengthPct float64
}

func (h *Handler) handleResults(w http.ResponseWriter, r *http.Request) {
	rows, err := h.store.ResultsByPair(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rankings := computeBradleyTerry(rows)
	view := make([]ResultsRow, 0, len(rows))
	for _, row := range rows {
		total := row.WinsA + row.WinsB + row.Draws
		if total == 0 {
			continue
		}
		view = append(view, ResultsRow{
			EngineA: row.EngineA,
			EngineB: row.EngineB,
			Wins:    row.WinsA,
			Losses:  row.WinsB,
			Draws:   row.Draws,
			Total:   total,
			WinPct:  float64(row.WinsA) * 100 / float64(total),
			LossPct: float64(row.WinsB) * 100 / float64(total),
			DrawPct: float64(row.Draws) * 100 / float64(total),
		})
	}
	sort.Slice(view, func(i, j int) bool {
		if view[i].Total == view[j].Total {
			if view[i].EngineA == view[j].EngineA {
				return view[i].EngineB < view[j].EngineB
			}
			return view[i].EngineA < view[j].EngineA
		}
		return view[i].Total > view[j].Total
	})
	_ = h.tpl.ExecuteTemplate(w, "results.html", map[string]any{"Rows": view, "Rankings": rankings})
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
			Name:       engineNames[i],
			Strength:   strength[i],
			ScorePct:   winScore * 100 / totalGames,
			Games:      int(totalGames),
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

func (h *Handler) handleAdminRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/admin/settings", http.StatusSeeOther)
}

func (h *Handler) handleAdminSettings(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.conf.GetConfig(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = h.tpl.ExecuteTemplate(w, "admin_settings.html", map[string]any{"Cfg": cfg})
}

func (h *Handler) handleAdminSettingsSave(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cfg, err := h.conf.GetConfig(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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

	cfg.MovetimeMS = movetime
	cfg.MaxPlies = maxPlies
	cfg.OpeningMin = openingMin
	cfg.BookEnabled = r.Form.Get("book_enabled") == "on"
	cfg.BookPath = strings.TrimSpace(r.Form.Get("book_path"))
	cfg.BookMaxPlies = bookMaxPlies

	if err := h.conf.UpdateConfig(r.Context(), cfg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/settings", http.StatusSeeOther)
}

func (h *Handler) handleAdminMatches(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.conf.GetConfig(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	results, err := h.store.ResultsByPair(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ranking := computeBradleyTerry(results)
	order := matchOrder(cfg, ranking)
	rows := buildMatchRows(cfg, order)
	strengths := matchStrengths(ranking, cfg)
	_ = h.tpl.ExecuteTemplate(w, "admin_matches.html", map[string]any{
		"Cfg":      cfg,
		"Rows":     rows,
		"Engines":  order,
		"Strengths": strengths,
		"PairCount": matchCellCount(rows),
	})
}

func (h *Handler) handleAdminMatchesSave(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cfg, err := h.conf.GetConfig(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cfg.EnabledPairs = parsePairsFromForm(r)
	if err := h.conf.UpdateConfig(r.Context(), cfg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/matches", http.StatusSeeOther)
}

func (h *Handler) handleAdminEngines(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.conf.GetConfig(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	view := buildAdminView(cfg, nil)
	_ = h.tpl.ExecuteTemplate(w, "admin_engines.html", view)
}

func (h *Handler) handleAdminEnginesSave(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cfg, err := h.conf.GetConfig(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	engines, view, ok := parseEnginesFromForm(r)
	if !ok {
		view.Cfg = cfg
		view.Cfg.Engines = engines
		_ = h.tpl.ExecuteTemplate(w, "admin_engines.html", view)
		return
	}
	if errMap := testEngines(r.Context(), engines); len(errMap) > 0 {
		view = buildAdminView(configstore.Config{Engines: engines, EnabledPairs: cfg.EnabledPairs}, errMap)
		_ = h.tpl.ExecuteTemplate(w, "admin_engines.html", view)
		return
	}

	cfg.Engines = engines
	cfg.EnabledPairs = filterPairs(cfg.EnabledPairs, engines)
	if err := h.conf.UpdateConfig(r.Context(), cfg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/engines", http.StatusSeeOther)
}

type EngineView struct {
	Index  int
	Name   string
	Path   string
	Args   string
	Init   string
	Active bool
	Error  string
}

type PairView struct {
	Index   int
	A       string
	B       string
	Label   string
	Enabled bool
}

type AdminView struct {
	Cfg     configstore.Config
	Engines []EngineView
	Pairs   []PairView
}

func buildAdminView(cfg configstore.Config, errMap map[int]string) AdminView {
	views := make([]EngineView, 0, len(cfg.Engines))
	for i, e := range cfg.Engines {
		view := EngineView{
			Index:  i,
			Name:   e.Name,
			Path:   e.Path,
			Args:   e.Args,
			Init:   e.Init,
			Active: e.Active,
		}
		if errMap != nil {
			view.Error = errMap[i]
		}
		views = append(views, view)
	}

	enabled := make(map[[2]string]bool)
	for _, p := range cfg.EnabledPairs {
		a, b := p.A, p.B
		if a > b {
			a, b = b, a
		}
		if a == "" || b == "" {
			continue
		}
		enabled[[2]string{a, b}] = true
	}

	pairs := make([]PairView, 0)
	for i := 0; i < len(cfg.Engines); i++ {
		for j := i; j < len(cfg.Engines); j++ {
			a := cfg.Engines[i].Name
			b := cfg.Engines[j].Name
			if a == "" || b == "" {
				continue
			}
			label := a
			if a == b {
				label = fmt.Sprintf("%s (selfplay)", a)
			} else {
				label = fmt.Sprintf("%s vs %s", a, b)
			}
			key := [2]string{minString(a, b), maxString(a, b)}
			pairs = append(pairs, PairView{
				A:       a,
				B:       b,
				Label:   label,
				Enabled: enabled[key],
			})
		}
	}
	for i := range pairs {
		pairs[i].Index = i
	}

	return AdminView{Cfg: cfg, Engines: views, Pairs: pairs}
}

func parseEnginesFromForm(r *http.Request) ([]configstore.EngineConfig, AdminView, bool) {
	count, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("engine_count")))
	if count < 0 {
		count = 0
	}

	engines := make([]configstore.EngineConfig, 0, count)
	viewEngines := make([]EngineView, 0, count)
	nameIndex := make(map[string]int)
	errMap := make(map[int]string)

	for i := 0; i < count; i++ {
		name := strings.TrimSpace(r.Form.Get(fmt.Sprintf("engine_name_%d", i)))
		path := strings.TrimSpace(r.Form.Get(fmt.Sprintf("engine_path_%d", i)))
		args := strings.TrimSpace(r.Form.Get(fmt.Sprintf("engine_args_%d", i)))
		init := r.Form.Get(fmt.Sprintf("engine_init_%d", i))
		activeVal := r.Form[fmt.Sprintf("engine_active_%d", i)]
		active := false
		if len(activeVal) > 0 {
			active = activeVal[len(activeVal)-1] == "1"
		}

		if name == "" && path == "" && args == "" && strings.TrimSpace(init) == "" {
			continue
		}

		if name == "" {
			if _, ok := errMap[len(engines)]; !ok {
				errMap[len(engines)] = "name required"
			}
		}
		if path == "" {
			if _, ok := errMap[len(engines)]; !ok {
				errMap[len(engines)] = "path required"
			}
		}
		if prev, ok := nameIndex[name]; ok && name != "" {
			errMap[prev] = "duplicate name"
			errMap[len(engines)] = "duplicate name"
		} else if name != "" {
			nameIndex[name] = len(engines)
		}

		engines = append(engines, configstore.EngineConfig{
			Name:   name,
			Path:   path,
			Args:   args,
			Init:   init,
			Active: active,
		})
		viewEngines = append(viewEngines, EngineView{
			Index:  len(engines) - 1,
			Name:   name,
			Path:   path,
			Args:   args,
			Init:   init,
			Active: active,
		})
	}

	for i := range viewEngines {
		if errText, ok := errMap[i]; ok {
			viewEngines[i].Error = errText
		}
	}

	if len(errMap) > 0 {
		cfg := configstore.Config{Engines: engines}
		return nil, AdminView{Cfg: cfg, Engines: viewEngines}, false
	}
	return engines, AdminView{Cfg: configstore.Config{Engines: engines}, Engines: viewEngines}, true
}

func parsePairsFromForm(r *http.Request) []configstore.PairConfig {
	count, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("pair_count")))
	if count < 0 {
		count = 0
	}
	seen := make(map[[2]string]bool)
	pairs := make([]configstore.PairConfig, 0, count)
	for i := 0; i < count; i++ {
		a := strings.TrimSpace(r.Form.Get(fmt.Sprintf("pair_a_%d", i)))
		b := strings.TrimSpace(r.Form.Get(fmt.Sprintf("pair_b_%d", i)))
		if a == "" || b == "" {
			continue
		}
		enabled := r.Form.Get(fmt.Sprintf("pair_enabled_%d", i)) == "on"
		if !enabled {
			continue
		}
		key := [2]string{minString(a, b), maxString(a, b)}
		if seen[key] {
			continue
		}
		seen[key] = true
		pairs = append(pairs, configstore.PairConfig{A: a, B: b})
	}
	return pairs
}

func minString(a, b string) string {
	if a < b {
		return a
	}
	return b
}

func maxString(a, b string) string {
	if a > b {
		return a
	}
	return b
}

type MatchCell struct {
	Index   int
	A       string
	B       string
	Label   string
	Enabled bool
}

type MatchRow struct {
	Engine string
	Cells  []MatchCell
}

func engineNames(engines []configstore.EngineConfig) []string {
	out := make([]string, 0, len(engines))
	for _, e := range engines {
		if e.Name == "" {
			continue
		}
		out = append(out, e.Name)
	}
	return out
}

func buildMatchRows(cfg configstore.Config, names []string) []MatchRow {
	if len(names) == 0 {
		return nil
	}
	enabled := make(map[[2]string]bool)
	for _, p := range cfg.EnabledPairs {
		a, b := p.A, p.B
		if a > b {
			a, b = b, a
		}
		if a == "" || b == "" {
			continue
		}
		enabled[[2]string{a, b}] = true
	}
	rows := make([]MatchRow, 0, len(names))
	index := 0
	for i, rowName := range names {
		row := MatchRow{Engine: rowName}
		for j := 0; j < len(names); j++ {
			colName := names[j]
			label := rowName
			if rowName == colName {
				label = fmt.Sprintf("%s (selfplay)", rowName)
			} else {
				label = fmt.Sprintf("%s vs %s", rowName, colName)
			}
			key := [2]string{minString(rowName, colName), maxString(rowName, colName)}
			row.Cells = append(row.Cells, MatchCell{
				Index:   index,
				A:       rowName,
				B:       colName,
				Label:   label,
				Enabled: enabled[key],
			})
			index++
		}
		if i < len(names) {
			rows = append(rows, row)
		}
	}
	return rows
}

func matchCellCount(rows []MatchRow) int {
	count := 0
	for _, row := range rows {
		count += len(row.Cells)
	}
	return count
}

func filterPairs(pairs []configstore.PairConfig, engines []configstore.EngineConfig) []configstore.PairConfig {
	valid := make(map[string]bool)
	for _, e := range engines {
		if e.Name != "" {
			valid[e.Name] = true
		}
	}
	out := make([]configstore.PairConfig, 0, len(pairs))
	for _, p := range pairs {
		if !valid[p.A] || !valid[p.B] {
			continue
		}
		out = append(out, p)
	}
	return out
}

func matchOrder(cfg configstore.Config, ranking []RankingRow) []string {
	order := engineNames(cfg.Engines)
	if len(order) == 0 {
		return order
	}
	if len(ranking) == 0 {
		return order
	}
	allowed := make(map[string]bool)
	for _, name := range order {
		allowed[name] = true
	}
	ranked := make([]string, 0, len(ranking))
	seen := make(map[string]bool)
	for _, r := range ranking {
		if !allowed[r.Name] {
			continue
		}
		ranked = append(ranked, r.Name)
		seen[r.Name] = true
	}
	for _, name := range order {
		if !seen[name] {
			ranked = append(ranked, name)
		}
	}
	return ranked
}

func matchStrengths(ranking []RankingRow, cfg configstore.Config) map[string]float64 {
	strengths := make(map[string]float64)
	allowed := make(map[string]bool)
	for _, name := range engineNames(cfg.Engines) {
		allowed[name] = true
	}
	for _, r := range ranking {
		if !allowed[r.Name] {
			continue
		}
		strengths[r.Name] = r.ScorePct
	}
	return strengths
}

func testEngines(ctx context.Context, engines []configstore.EngineConfig) map[int]string {
	errMap := make(map[int]string)
	for i, e := range engines {
		if e.Path == "" {
			continue
		}
		eng := engine.NewUCIEngine(e.Path, strings.Fields(e.Args))
		testCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := eng.Start(testCtx); err != nil {
			errMap[i] = err.Error()
			cancel()
			continue
		}
		if err := eng.IsReady(testCtx); err != nil {
			errMap[i] = err.Error()
		}
		_ = eng.Close()
		cancel()
	}
	return errMap
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
