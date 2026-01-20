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
	"tethys/internal/db"
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
	_ = h.tpl.ExecuteTemplate(w, "global_settings.html", map[string]any{
		"Cfg":     cfg,
		"IsAdmin": true,
		"Page":    "settings",
	})
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
	engines, err := h.store.ListEngines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	matchups, err := h.store.ListMatchups(r.Context())
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
	order := matchOrder(engines, ranking)
	orderedEngines := orderEngines(engines, order)
	rows := buildMatchRows(orderedEngines, matchups)
	strengths := matchStrengths(ranking, orderedEngines)
	_ = h.tpl.ExecuteTemplate(w, "match_settings.html", map[string]any{
		"Cfg":       cfg,
		"Rows":      rows,
		"Engines":   buildEngineHeaders(orderedEngines),
		"Strengths": strengths,
		"PairCount": matchCellCount(rows),
		"IsAdmin":   true,
		"Page":      "matches",
	})
}

func (h *Handler) handleAdminMatchesSave(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	matchups := parsePairsFromForm(r)
	if err := h.store.ReplaceMatchups(r.Context(), matchups); err != nil {
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
	engines, err := h.store.ListEngines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	gameCounts, err := h.store.EngineGameCounts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	matchupCounts, err := h.store.EngineMatchupCounts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	matchups, err := h.store.ListMatchups(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	view := buildAdminView(cfg, engines, matchups, nil, gameCounts, matchupCounts)
	view.IsAdmin = true
	view.Page = "engines"
	_ = h.tpl.ExecuteTemplate(w, "engine_settings.html", view)
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

	parsed, view, ok := parseEnginesFromForm(r)
	if !ok {
		view.IsAdmin = true
		view.Page = "engines"
		_ = h.tpl.ExecuteTemplate(w, "engine_settings.html", view)
		return
	}

	if errMap := testEngines(r.Context(), parsed); len(errMap) > 0 {
		gameCounts, err := h.store.EngineGameCounts(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		matchupCounts, err := h.store.EngineMatchupCounts(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		view.Engines = buildEngineViewsFromList(parsed, errMap, gameCounts, matchupCounts)
		view.IsAdmin = true
		view.Page = "engines"
		_ = h.tpl.ExecuteTemplate(w, "engine_settings.html", view)
		return
	}

	current, err := h.store.ListEngines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	gameCounts, err := h.store.EngineGameCounts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	matchupCounts, err := h.store.EngineMatchupCounts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	seen := make(map[int64]bool)
	for _, e := range parsed {
		if e.ID == 0 {
			if _, err := h.store.InsertEngine(r.Context(), e); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			continue
		}
		seen[e.ID] = true
		if err := h.store.UpdateEngine(r.Context(), e); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	errByID := make(map[int64]string)
	for _, e := range current {
		if e.ID == 0 || seen[e.ID] {
			continue
		}
		if gameCounts[e.ID] > 0 {
			errByID[e.ID] = "engine used by games"
			continue
		}
		if matchupCounts[e.ID] > 0 {
			errByID[e.ID] = "engine used by matchups"
			continue
		}
		if err := h.store.DeleteEngine(r.Context(), e.ID); err != nil {
			errByID[e.ID] = err.Error()
		}
	}
	if len(errByID) > 0 {
		fresh, err := h.store.ListEngines(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		matchups, err := h.store.ListMatchups(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		view = buildAdminView(cfg, fresh, matchups, errByID, gameCounts, matchupCounts)
		view.IsAdmin = true
		view.Page = "engines"
		_ = h.tpl.ExecuteTemplate(w, "engine_settings.html", view)
		return
	}

	http.Redirect(w, r, "/admin/engines", http.StatusSeeOther)
}

func (h *Handler) handleAdminEnginePrune(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	idStr := strings.TrimSpace(r.Form.Get("engine_id"))
	engineID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || engineID == 0 {
		http.Error(w, "invalid engine id", http.StatusBadRequest)
		return
	}
	if _, err := h.store.DeleteGamesByEngine(r.Context(), engineID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := h.store.DeleteMatchupsByEngine(r.Context(), engineID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/engines", http.StatusSeeOther)
}

func (h *Handler) isAdminRequest(w http.ResponseWriter, r *http.Request) bool {
	if h.adminToken == "" {
		return false
	}
	if token := strings.TrimSpace(r.URL.Query().Get("token")); token != "" {
		if tokensEqual(token, h.adminToken) {
			h.setAdminCookie(w)
			return true
		}
		return false
	}
	cookie, err := r.Cookie("tethys_admin_token")
	if err != nil || cookie.Value == "" {
		return false
	}
	return tokensEqual(cookie.Value, h.adminToken)
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
	ID       int64
	Index    int
	Name     string
	Path     string
	Args     string
	Init     string
	Error    string
	Games    int
	Matchups int
}

type PairView struct {
	Index   int
	AID     int64
	BID     int64
	AName   string
	BName   string
	Label   string
	Enabled bool
}

type AdminView struct {
	Cfg     configstore.Config
	Engines []EngineView
	Pairs   []PairView
	IsAdmin bool
	Page    string
}

func buildAdminView(cfg configstore.Config, engines []db.Engine, matchups []db.Matchup, errByID map[int64]string, gameCounts map[int64]int, matchupCounts map[int64]int) AdminView {
	views := make([]EngineView, 0, len(engines))
	for i, e := range engines {
		view := EngineView{
			ID:       e.ID,
			Index:    i,
			Name:     e.Name,
			Path:     e.Path,
			Args:     e.Args,
			Init:     e.Init,
			Games:    gameCounts[e.ID],
			Matchups: matchupCounts[e.ID],
		}
		if errByID != nil {
			view.Error = errByID[e.ID]
		}
		views = append(views, view)
	}

	nameByID := make(map[int64]string)
	for _, e := range engines {
		nameByID[e.ID] = e.Name
	}

	enabled := make(map[[2]int64]bool)
	for _, p := range matchups {
		a, b := p.EngineAID, p.EngineBID
		if a == 0 || b == 0 {
			continue
		}
		if a > b {
			a, b = b, a
		}
		enabled[[2]int64{a, b}] = true
	}

	pairs := make([]PairView, 0)
	for i := 0; i < len(engines); i++ {
		for j := i; j < len(engines); j++ {
			aID := engines[i].ID
			bID := engines[j].ID
			aName := nameByID[aID]
			bName := nameByID[bID]
			if aID == 0 || bID == 0 {
				continue
			}
			label := aName
			if aID == bID {
				label = fmt.Sprintf("%s (selfplay)", aName)
			} else {
				label = fmt.Sprintf("%s vs %s", aName, bName)
			}
			key := [2]int64{minInt(aID, bID), maxInt(aID, bID)}
			pairs = append(pairs, PairView{
				AID:     aID,
				BID:     bID,
				AName:   aName,
				BName:   bName,
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

func parseEnginesFromForm(r *http.Request) ([]db.Engine, AdminView, bool) {
	count, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("engine_count")))
	if count < 0 {
		count = 0
	}

	engines := make([]db.Engine, 0, count)
	viewEngines := make([]EngineView, 0, count)
	nameIndex := make(map[string]int)
	errMap := make(map[int]string)

	for i := 0; i < count; i++ {
		idStr := strings.TrimSpace(r.Form.Get(fmt.Sprintf("engine_id_%d", i)))
		id, _ := strconv.ParseInt(idStr, 10, 64)
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

		engines = append(engines, db.Engine{
			ID:   id,
			Name: name,
			Path: path,
			Args: args,
			Init: init,
		})
		viewEngines = append(viewEngines, EngineView{
			ID:    id,
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
		return nil, AdminView{Engines: viewEngines}, false
	}
	return engines, AdminView{Engines: viewEngines}, true
}

func parsePairsFromForm(r *http.Request) []db.Matchup {
	count, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("pair_count")))
	if count < 0 {
		count = 0
	}
	seen := make(map[[2]int64]bool)
	pairs := make([]db.Matchup, 0, count)
	for i := 0; i < count; i++ {
		aStr := strings.TrimSpace(r.Form.Get(fmt.Sprintf("pair_a_%d", i)))
		bStr := strings.TrimSpace(r.Form.Get(fmt.Sprintf("pair_b_%d", i)))
		aID, _ := strconv.ParseInt(aStr, 10, 64)
		bID, _ := strconv.ParseInt(bStr, 10, 64)
		if aID == 0 || bID == 0 {
			continue
		}
		enabled := r.Form.Get(fmt.Sprintf("pair_enabled_%d", i)) == "on"
		if !enabled {
			continue
		}
		key := [2]int64{minInt(aID, bID), maxInt(aID, bID)}
		if seen[key] {
			continue
		}
		seen[key] = true
		pairs = append(pairs, db.Matchup{EngineAID: key[0], EngineBID: key[1]})
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

type EngineHeader struct {
	ID   int64
	Name string
}

func buildEngineHeaders(engines []db.Engine) []EngineHeader {
	headers := make([]EngineHeader, 0, len(engines))
	for _, e := range engines {
		headers = append(headers, EngineHeader{ID: e.ID, Name: e.Name})
	}
	return headers
}

func orderEngines(engines []db.Engine, order []string) []db.Engine {
	byName := make(map[string]db.Engine)
	for _, e := range engines {
		byName[e.Name] = e
	}
	ordered := make([]db.Engine, 0, len(engines))
	seen := make(map[string]bool)
	for _, name := range order {
		if e, ok := byName[name]; ok {
			ordered = append(ordered, e)
			seen[name] = true
		}
	}
	for _, e := range engines {
		if !seen[e.Name] {
			ordered = append(ordered, e)
		}
	}
	return ordered
}

func buildEngineViewsFromList(engines []db.Engine, errByIndex map[int]string, gameCounts map[int64]int, matchupCounts map[int64]int) []EngineView {
	views := make([]EngineView, 0, len(engines))
	for i, e := range engines {
		view := EngineView{
			ID:       e.ID,
			Index:    i,
			Name:     e.Name,
			Path:     e.Path,
			Args:     e.Args,
			Init:     e.Init,
			Games:    gameCounts[e.ID],
			Matchups: matchupCounts[e.ID],
		}
		if errByIndex != nil {
			view.Error = errByIndex[i]
		}
		views = append(views, view)
	}
	return views
}

func minInt(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

type MatchCell struct {
	Index   int
	AID     int64
	BID     int64
	Label   string
	Enabled bool
}

type MatchRow struct {
	EngineID   int64
	EngineName string
	Cells      []MatchCell
}

func engineNames(engines []db.Engine) []string {
	out := make([]string, 0, len(engines))
	for _, e := range engines {
		if e.Name == "" {
			continue
		}
		out = append(out, e.Name)
	}
	return out
}

func buildMatchRows(engines []db.Engine, matchups []db.Matchup) []MatchRow {
	if len(engines) == 0 {
		return nil
	}
	enabled := make(map[[2]int64]bool)
	for _, p := range matchups {
		a, b := p.EngineAID, p.EngineBID
		if a == 0 || b == 0 {
			continue
		}
		if a > b {
			a, b = b, a
		}
		enabled[[2]int64{a, b}] = true
	}
	rows := make([]MatchRow, 0, len(engines))
	index := 0
	for i, rowEng := range engines {
		row := MatchRow{EngineID: rowEng.ID, EngineName: rowEng.Name}
		for j := 0; j < len(engines); j++ {
			colEng := engines[j]
			label := rowEng.Name
			if rowEng.ID == colEng.ID {
				label = fmt.Sprintf("%s (selfplay)", rowEng.Name)
			} else {
				label = fmt.Sprintf("%s vs %s", rowEng.Name, colEng.Name)
			}
			key := [2]int64{minInt(rowEng.ID, colEng.ID), maxInt(rowEng.ID, colEng.ID)}
			row.Cells = append(row.Cells, MatchCell{
				Index:   index,
				AID:     rowEng.ID,
				BID:     colEng.ID,
				Label:   label,
				Enabled: enabled[key],
			})
			index++
		}
		if i < len(engines) {
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

func matchOrder(engines []db.Engine, ranking []RankingRow) []string {
	order := engineNames(engines)
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

func matchStrengths(ranking []RankingRow, engines []db.Engine) map[string]float64 {
	strengths := make(map[string]float64)
	allowed := make(map[string]bool)
	for _, name := range engineNames(engines) {
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

func testEngines(ctx context.Context, engines []db.Engine) map[int]string {
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
