package web

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"tethys/internal/db"
	"tethys/internal/engine"
)

func (h *Handler) handleAdminLogout(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/admin/settings", http.StatusSeeOther)
}

func (h *Handler) handleAdminRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/admin/settings", http.StatusSeeOther)
}

func (h *Handler) handleAdminSettings(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.store.GetSettings(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	engines, err := h.store.ListEngines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	books, err := listBookOptions(h.booksDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bookName := ""
	if strings.TrimSpace(cfg.GameBookPath) != "" {
		bookName = filepath.Base(cfg.GameBookPath)
	}
	_ = h.tpl.ExecuteTemplate(w, "global_settings.html", map[string]any{
		"Cfg":      cfg,
		"Engines":  engines,
		"Books":    books,
		"BookName": bookName,
		"Page":     "settings",
	})
}

func (h *Handler) handleAdminSettingsSave(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cfg, err := h.store.GetSettings(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	openingMin, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("opening_min")))
	if openingMin <= 0 {
		openingMin = cfg.OpeningMin
		if openingMin <= 0 {
			openingMin = 20
		}
	}
	analysisDepth, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("analysis_depth")))
	if analysisDepth <= 0 {
		analysisDepth = cfg.AnalysisDepth
		if analysisDepth <= 0 {
			analysisDepth = 12
		}
	}
	analysisEngineID := cfg.AnalysisEngineID
	if raw := strings.TrimSpace(r.Form.Get("analysis_engine_id")); raw != "" {
		analysisEngineID, _ = strconv.ParseInt(raw, 10, 64)
	}
	gameMovetime, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("game_movetime_ms")))
	if gameMovetime <= 0 {
		gameMovetime = cfg.GameMovetimeMS
		if gameMovetime <= 0 {
			gameMovetime = 100
		}
	}
	gameSlack, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("game_slack_ms")))
	if gameSlack <= 0 {
		gameSlack = cfg.GameSlackMS
		if gameSlack <= 0 {
			gameSlack = 100
		}
	}
	matchSoftScale, _ := strconv.Atoi(strings.TrimSpace(r.Form.Get("match_soft_scale")))
	if matchSoftScale <= 0 {
		matchSoftScale = cfg.MatchSoftScale
		if matchSoftScale <= 0 {
			matchSoftScale = 300
		}
	}
	matchAllowMirror := cfg.MatchAllowMirror
	if _, ok := r.Form["match_allow_mirror"]; ok {
		raw := strings.TrimSpace(r.Form.Get("match_allow_mirror"))
		matchAllowMirror = raw == "1" || strings.EqualFold(raw, "true") || strings.EqualFold(raw, "on")
	}
	gameBook := ""
	if vals, ok := r.Form["game_book"]; ok {
		if len(vals) > 0 {
			gameBook = strings.TrimSpace(vals[0])
		}
		if gameBook == "(none)" {
			gameBook = ""
		}
	} else if strings.TrimSpace(cfg.GameBookPath) != "" {
		gameBook = filepath.Base(cfg.GameBookPath)
	}
	gameBookPath := ""
	if gameBook != "" {
		options, err := listBookOptions(h.booksDir)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		valid := false
		for _, name := range options {
			if name == gameBook {
				valid = true
				break
			}
		}
		if !valid {
			http.Error(w, "invalid book selection", http.StatusBadRequest)
			return
		}
		gameBookPath = filepath.Join(h.booksDir, gameBook)
	}

	cfg.OpeningMin = openingMin
	cfg.AnalysisDepth = analysisDepth
	cfg.AnalysisEngineID = analysisEngineID
	cfg.GameMovetimeMS = gameMovetime
	cfg.GameSlackMS = gameSlack
	cfg.GameBookPath = gameBookPath
	cfg.MatchSoftScale = matchSoftScale
	cfg.MatchAllowMirror = matchAllowMirror

	if err := h.store.UpdateSettings(r.Context(), cfg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/settings", http.StatusSeeOther)
}

func (h *Handler) handleAdminMatches(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.store.GetSettings(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	books, err := listBookOptions(h.booksDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bookName := ""
	if strings.TrimSpace(cfg.GameBookPath) != "" {
		bookName = filepath.Base(cfg.GameBookPath)
	}
	_ = h.tpl.ExecuteTemplate(w, "match_settings.html", map[string]any{
		"Cfg":      cfg,
		"Books":    books,
		"BookName": bookName,
		"Page":     "matches",
	})
}

func (h *Handler) handleAdminEngines(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.store.GetSettings(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	engines, err := h.store.ListEngines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	engineBinaries, err := listEngineBinaries(h.enginesDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	gameCounts, err := h.store.EngineGameCounts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	view := buildAdminView(cfg, engines, nil, gameCounts)
	view.Page = "engines"
	view.UnusedEngines = buildUnusedEngineViews(h.enginesDir, engines, engineBinaries)
	_ = h.tpl.ExecuteTemplate(w, "engine_settings.html", view)
}

func (h *Handler) handleAdminEnginesSave(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	current, err := h.store.ListEngines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	currentByID := make(map[int64]db.Engine, len(current))
	for _, e := range current {
		currentByID[e.ID] = e
	}
	gameCounts, err := h.store.EngineGameCounts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	parsed, view, ok := parseEnginesFromForm(r, currentByID)
	if !ok {
		for i := range view.Engines {
			id := view.Engines[i].ID
			view.Engines[i].Games = gameCounts[id]
		}
		view.Page = "engines"
		if bins, err := listEngineBinaries(h.enginesDir); err == nil {
			view.UnusedEngines = buildUnusedEngineViews(h.enginesDir, current, bins)
		}
		_ = h.tpl.ExecuteTemplate(w, "engine_settings.html", view)
		return
	}

	if errMap := testEngines(r.Context(), parsed); len(errMap) > 0 {
		view.Engines = buildEngineViewsFromList(parsed, errMap, gameCounts)
		view.Page = "engines"
		if bins, err := listEngineBinaries(h.enginesDir); err == nil {
			view.UnusedEngines = buildUnusedEngineViews(h.enginesDir, current, bins)
		}
		_ = h.tpl.ExecuteTemplate(w, "engine_settings.html", view)
		return
	}
	seen := make(map[int64]bool)
	addedNew := false
	for _, e := range parsed {
		if e.ID == 0 {
			if _, err := h.store.InsertEngine(r.Context(), e); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			addedNew = true
			continue
		}
		seen[e.ID] = true
		if err := h.store.UpdateEngine(r.Context(), e); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if addedNew {
		_ = h.store.ClearGameQueue(r.Context())
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
		cfg, err := h.store.GetSettings(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		view = buildAdminView(cfg, fresh, errByID, gameCounts)
		view.Page = "engines"
		if bins, err := listEngineBinaries(h.enginesDir); err == nil {
			view.UnusedEngines = buildUnusedEngineViews(h.enginesDir, fresh, bins)
		}
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
	_ = h.store.ClearGameQueue(r.Context())
	if _, err := h.store.DeleteGamesByEngine(r.Context(), engineID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := h.store.DeleteEvalsByEngine(r.Context(), engineID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := h.store.DeleteEngine(r.Context(), engineID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/engines", http.StatusSeeOther)
}

func (h *Handler) handleAdminEngineDuplicate(w http.ResponseWriter, r *http.Request) {
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
	name := strings.TrimSpace(r.Form.Get("engine_name"))
	args := strings.TrimSpace(r.Form.Get("engine_args"))
	init := r.Form.Get("engine_init")
	original, err := h.store.EngineByID(r.Context(), engineID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if name == "" {
		name = fmt.Sprintf("copy of %s", strings.TrimSpace(original.Name))
	}
	unique, err := h.uniqueEngineName(r.Context(), name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = h.store.InsertEngine(r.Context(), db.Engine{
		Name: unique,
		Path: original.Path,
		Args: args,
		Init: init,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = h.store.ClearGameQueue(r.Context())
	http.Redirect(w, r, "/admin/engines", http.StatusSeeOther)
}

func (h *Handler) handleAdminEngineRename(w http.ResponseWriter, r *http.Request) {
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
	name := strings.TrimSpace(r.Form.Get("engine_name"))
	if name == "" {
		http.Error(w, "engine name required", http.StatusBadRequest)
		return
	}
	original, err := h.store.EngineByID(r.Context(), engineID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if name != original.Name {
		engines, err := h.store.ListEngines(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, engine := range engines {
			if engine.ID != engineID && strings.TrimSpace(engine.Name) == name {
				http.Error(w, "engine name already exists", http.StatusBadRequest)
				return
			}
		}
	}
	if err := h.store.UpdateEngine(r.Context(), db.Engine{
		ID:   original.ID,
		Name: name,
		Path: original.Path,
		Args: original.Args,
		Init: original.Init,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = h.store.ClearGameQueue(r.Context())
	http.Redirect(w, r, "/admin/engines", http.StatusSeeOther)
}

func (h *Handler) handleAdminEngineAddUnused(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(r.Form.Get("engine_name"))
	args := strings.TrimSpace(r.Form.Get("engine_args"))
	init := r.Form.Get("engine_init")
	binary := strings.TrimSpace(r.Form.Get("engine_binary"))
	if binary == "" {
		http.Error(w, "engine binary required", http.StatusBadRequest)
		return
	}
	options, err := listEngineBinaries(h.enginesDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	valid := false
	for _, opt := range options {
		if opt == binary {
			valid = true
			break
		}
	}
	if !valid {
		http.Error(w, "invalid engine binary", http.StatusBadRequest)
		return
	}
	current, err := h.store.ListEngines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	enginePath := filepath.Join(h.enginesDir, binary)
	for _, engine := range current {
		if filepath.Clean(engine.Path) == enginePath {
			http.Error(w, "engine already exists", http.StatusBadRequest)
			return
		}
	}
	path := enginePath
	if name == "" {
		name = binary
	}
	unique, err := h.uniqueEngineName(r.Context(), name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = h.store.InsertEngine(r.Context(), db.Engine{
		Name: unique,
		Path: path,
		Args: args,
		Init: init,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = h.store.ClearGameQueue(r.Context())
	http.Redirect(w, r, "/admin/engines", http.StatusSeeOther)
}

func (h *Handler) handleAdminEngineDeleteUnused(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	binary := strings.TrimSpace(r.Form.Get("engine_binary"))
	if binary == "" {
		http.Error(w, "engine binary required", http.StatusBadRequest)
		return
	}
	options, err := listEngineBinaries(h.enginesDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	valid := false
	for _, opt := range options {
		if opt == binary {
			valid = true
			break
		}
	}
	if !valid {
		http.Error(w, "invalid engine binary", http.StatusBadRequest)
		return
	}
	current, err := h.store.ListEngines(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	enginePath := filepath.Join(h.enginesDir, binary)
	for _, engine := range current {
		if filepath.Clean(engine.Path) == enginePath {
			http.Error(w, "engine already exists", http.StatusBadRequest)
			return
		}
	}
	path := enginePath
	if err := os.Remove(path); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/engines", http.StatusSeeOther)
}

type EngineView struct {
	ID    int64
	Index int
	Name  string
	Path  string
	Args  string
	Init  string
	Error string
	Games int
}

type UnusedEngineView struct {
	Binary      string
	DefaultName string
}

type AdminView struct {
	Cfg            db.Settings
	Engines        []EngineView
	Page           string
	EngineBinaries []string
	UnusedEngines  []UnusedEngineView
}

func buildAdminView(cfg db.Settings, engines []db.Engine, errByID map[int64]string, gameCounts map[int64]int) AdminView {
	views := make([]EngineView, 0, len(engines))
	for i, e := range engines {
		view := EngineView{
			ID:    e.ID,
			Index: i,
			Name:  e.Name,
			Path:  e.Path,
			Args:  e.Args,
			Init:  e.Init,
			Games: gameCounts[e.ID],
		}
		if errByID != nil {
			view.Error = errByID[e.ID]
		}
		views = append(views, view)
	}

	return AdminView{Cfg: cfg, Engines: views}
}

func buildUnusedEngineViews(enginesDir string, engines []db.Engine, binaries []string) []UnusedEngineView {
	used := make(map[string]bool, len(engines))
	for _, engine := range engines {
		cleanPath := filepath.Clean(strings.TrimSpace(engine.Path))
		if cleanPath == "" {
			continue
		}
		base := filepath.Base(cleanPath)
		if base == "" {
			continue
		}
		if cleanPath == filepath.Join(enginesDir, base) {
			used[base] = true
		}
	}
	views := make([]UnusedEngineView, 0, len(binaries))
	for _, binary := range binaries {
		if used[binary] {
			continue
		}
		views = append(views, UnusedEngineView{Binary: binary, DefaultName: binary})
	}
	return views
}

func parseEnginesFromForm(r *http.Request, existing map[int64]db.Engine) ([]db.Engine, AdminView, bool) {
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

func listEngineBinaries(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	options := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		options = append(options, name)
	}
	sort.Strings(options)
	return options, nil
}

func engineNameFromPath(path string) string {
	base := filepath.Base(strings.TrimSpace(path))
	if base == "" || base == "." || base == "/" {
		return "engine"
	}
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	if name == "" {
		return base
	}
	return name
}

func (h *Handler) uniqueEngineName(ctx context.Context, base string) (string, error) {
	name := strings.TrimSpace(base)
	if name == "" {
		name = "engine"
	}
	engines, err := h.store.ListEngines(ctx)
	if err != nil {
		return "", err
	}
	seen := make(map[string]bool, len(engines))
	for _, e := range engines {
		seen[strings.TrimSpace(e.Name)] = true
	}
	if !seen[name] {
		return name, nil
	}
	for i := 2; i < 1000; i++ {
		candidate := fmt.Sprintf("%s (%d)", name, i)
		if !seen[candidate] {
			return candidate, nil
		}
	}
	return fmt.Sprintf("%s (%d)", name, time.Now().Unix()), nil
}

func buildEngineViewsFromList(engines []db.Engine, errByIndex map[int]string, gameCounts map[int64]int) []EngineView {
	views := make([]EngineView, 0, len(engines))
	for i, e := range engines {
		view := EngineView{
			ID:    e.ID,
			Index: i,
			Name:  e.Name,
			Path:  e.Path,
			Args:  e.Args,
			Init:  e.Init,
			Games: gameCounts[e.ID],
		}
		if errByIndex != nil {
			view.Error = errByIndex[i]
		}
		views = append(views, view)
	}
	return views
}

func listBookOptions(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	options := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		options = append(options, name)
	}
	sort.Strings(options)
	return options, nil
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
