package web

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"tethys/internal/configstore"
	"tethys/internal/engine"
)

func (h *Handler) handleAdminLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: "tethys_admin_token", Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (h *Handler) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.adminToken == "" {
			http.Error(w, "/admin disabled (no admin token)", http.StatusForbidden)
			return
		}
		if token := strings.TrimSpace(r.URL.Query().Get("token")); token != "" {
			if tokensEqual(token, h.adminToken) {
				h.setAdminCookie(w)
				next(w, r)
				return
			}
			http.Error(w, "invalid admin token", http.StatusUnauthorized)
			return
		}
		cookie, err := r.Cookie("tethys_admin_token")
		if err != nil || cookie.Value == "" {
			http.Error(w, "missing admin token (add ?token=...) to the URL", http.StatusUnauthorized)
			return
		}
		if !tokensEqual(cookie.Value, h.adminToken) {
			http.Error(w, "invalid admin token", http.StatusUnauthorized)
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
		"Cfg":       cfg,
		"Rows":      rows,
		"Engines":   order,
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

func (h *Handler) handleAdminRestart(w http.ResponseWriter, r *http.Request) {
	h.r.Restart()
	http.Redirect(w, r, "/admin", http.StatusSeeOther)
}

func (h *Handler) setAdminCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     "tethys_admin_token",
		Value:    h.adminToken,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func tokensEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

type EngineView struct {
	Index int
	Name  string
	Path  string
	Args  string
	Init  string
	Error string
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
			Index: i,
			Name:  e.Name,
			Path:  e.Path,
			Args:  e.Args,
			Init:  e.Init,
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
			Name: name,
			Path: path,
			Args: args,
			Init: init,
		})
		viewEngines = append(viewEngines, EngineView{
			Index: len(engines) - 1,
			Name:  name,
			Path:  path,
			Args:  args,
			Init:  init,
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
