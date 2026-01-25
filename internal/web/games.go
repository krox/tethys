package web

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/notnil/chess"

	"tethys/internal/db"
)

type MatchupRow struct {
	AID        int64
	BID        int64
	A          string
	B          string
	MovetimeMS int
	Wins       int
	Losses     int
	Draws      int
	PointsA    float64
	PointsB    float64
	Total      int
	WinPct     float64
	LossPct    float64
	DrawPct    float64
	ShowNames  bool
	RowSpan    int
}

type ResultRow struct {
	Label       string
	Result      string
	Termination string
	Count       int
}

type SearchView struct {
	EngineID     int64
	WhiteID      int64
	BlackID      int64
	AllowSwap    bool
	Movetime     string
	Result       string
	Termination  string
	Limit        int
	Total        int
	Rows         []db.GameDetail
	Engines      []db.Engine
	Results      []string
	Terminations []string
}

func (h *Handler) handleGames(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	matchups, err := h.store.ListMatchupSummaries(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resultSummaries, err := h.store.ListResultSummaries(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	searchView, err := buildSearchView(ctx, h.store, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rows := make([]MatchupRow, 0, len(matchups))
	for _, m := range matchups {
		total := m.WinsA + m.WinsB + m.Draws
		if total == 0 {
			continue
		}
		rows = append(rows, MatchupRow{
			AID:        m.AID,
			BID:        m.BID,
			A:          m.A,
			B:          m.B,
			MovetimeMS: m.MovetimeMS,
			Wins:       m.WinsA,
			Losses:     m.WinsB,
			Draws:      m.Draws,
			PointsA:    float64(m.WinsA) + 0.5*float64(m.Draws),
			PointsB:    float64(m.WinsB) + 0.5*float64(m.Draws),
			Total:      total,
			WinPct:     float64(m.WinsA) * 100 / float64(total),
			LossPct:    float64(m.WinsB) * 100 / float64(total),
			DrawPct:    float64(m.Draws) * 100 / float64(total),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].A == rows[j].A {
			if rows[i].B == rows[j].B {
				return rows[i].MovetimeMS < rows[j].MovetimeMS
			}
			return rows[i].B < rows[j].B
		}
		return rows[i].A < rows[j].A
	})

	groupSizes := make(map[string]int)
	for _, row := range rows {
		key := row.A + "\x00" + row.B
		groupSizes[key]++
	}
	seen := make(map[string]bool)
	for i := range rows {
		key := rows[i].A + "\x00" + rows[i].B
		if !seen[key] {
			rows[i].ShowNames = true
			rows[i].RowSpan = groupSizes[key]
			seen[key] = true
		}
	}
	_ = h.tpl.ExecuteTemplate(w, "game_database.html", map[string]any{
		"Rows":       rows,
		"ResultRows": buildResultRows(resultSummaries),
		"Search":     searchView,
		"IsAdmin":    h.isAdminRequest(w, r),
		"Page":       "games",
	})
}

func buildSearchView(ctx context.Context, store *db.Store, r *http.Request) (SearchView, error) {
	q := r.URL.Query()
	engineID, _ := strconv.ParseInt(strings.TrimSpace(q.Get("engine")), 10, 64)
	result := strings.TrimSpace(q.Get("result"))
	termination := strings.TrimSpace(q.Get("termination"))
	movetimeStr := strings.TrimSpace(q.Get("movetime"))
	limit := 20
	if limitStr := strings.TrimSpace(q.Get("limit")); limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v > 0 {
			limit = v
		}
	}
	movetime := 0
	if movetimeStr != "" {
		if v, err := strconv.Atoi(movetimeStr); err == nil {
			movetime = v
		}
	}

	whiteID, _ := strconv.ParseInt(strings.TrimSpace(q.Get("white")), 10, 64)
	blackID, _ := strconv.ParseInt(strings.TrimSpace(q.Get("black")), 10, 64)
	allowSwap := q.Get("swap") == "on"

	filter := db.GameSearchFilter{
		EngineID:    engineID,
		WhiteID:     whiteID,
		BlackID:     blackID,
		AllowSwap:   allowSwap,
		MovetimeMS:  movetime,
		Result:      result,
		Termination: termination,
	}
	total, rows, err := store.SearchGames(ctx, filter, limit)
	if err != nil {
		return SearchView{}, err
	}
	engines, err := store.ListEngines(ctx)
	if err != nil {
		return SearchView{}, err
	}
	results, err := store.ListResults(ctx)
	if err != nil {
		return SearchView{}, err
	}
	terminations, err := store.ListTerminations(ctx)
	if err != nil {
		return SearchView{}, err
	}
	return SearchView{
		EngineID:     engineID,
		WhiteID:      whiteID,
		BlackID:      blackID,
		AllowSwap:    allowSwap,
		Movetime:     movetimeStr,
		Result:       result,
		Termination:  termination,
		Limit:        limit,
		Total:        total,
		Rows:         rows,
		Engines:      engines,
		Results:      results,
		Terminations: terminations,
	}, nil
}

func (h *Handler) handleResultDelete(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result := strings.TrimSpace(r.Form.Get("result"))
	termination := strings.TrimSpace(r.Form.Get("termination"))
	if result == "" {
		http.Error(w, "missing result", http.StatusBadRequest)
		return
	}
	if _, err := h.store.DeleteResultGames(r.Context(), result, termination); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/games", http.StatusSeeOther)
}

func (h *Handler) handleResultDownload(w http.ResponseWriter, r *http.Request) {
	result := strings.TrimSpace(r.URL.Query().Get("result"))
	termination := strings.TrimSpace(r.URL.Query().Get("termination"))
	if result == "" {
		http.Error(w, "missing result", http.StatusBadRequest)
		return
	}
	lines, err := h.store.ResultMovesLines(r.Context(), result, termination)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	label := sanitizeFilename(resultLabel(result, termination))
	filename := fmt.Sprintf("result-%s.txt", label)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	_, _ = w.Write([]byte(lines))
}

func buildResultRows(rows []db.ResultSummary) []ResultRow {
	out := make([]ResultRow, 0, len(rows))
	for _, row := range rows {
		label := resultLabel(row.Result, row.Termination)
		out = append(out, ResultRow{
			Label:       label,
			Result:      row.Result,
			Termination: row.Termination,
			Count:       row.Count,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Label < out[j].Label
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func resultLabel(result, termination string) string {
	result = strings.TrimSpace(result)
	termination = strings.TrimSpace(termination)
	if result == "1/2-1/2" {
		if termination != "" {
			return termination
		}
		return "Draw"
	}
	if result == "1-0" {
		if termination != "" {
			return "White wins — " + termination
		}
		return "White wins"
	}
	if result == "0-1" {
		if termination != "" {
			return "Black wins — " + termination
		}
		return "Black wins"
	}
	if termination != "" {
		return termination
	}
	return "Unfinished"
}

func (h *Handler) handleGameView(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimSpace(r.URL.Query().Get("id"))
	if idStr == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	game, err := h.store.GetGame(r.Context(), id)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	view, err := buildGameView(game)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	view.IsAdmin = h.isAdminRequest(w, r)
	view.Page = "games"
	_ = h.tpl.ExecuteTemplate(w, "game_viewer.html", view)
}

type GameMoveView struct {
	Index int
	UCI   string
	SAN   string
}

type GamePositionView struct {
	Index int
	Board [][]SquareView
}

type GameView struct {
	ID          int64
	PlayedAt    string
	White       string
	Black       string
	MovetimeMS  int
	Result      string
	Termination string
	Moves       []GameMoveView
	Positions   []GamePositionView
	IsAdmin     bool
	Page        string
}

func buildGameView(game db.GameDetail) (GameView, error) {
	pos := chess.StartingPosition()
	positions := []GamePositionView{{Index: 0, Board: boardFromPosition(pos)}}
	moves := make([]GameMoveView, 0)

	if strings.TrimSpace(game.MovesUCI) != "" {
		parts := strings.Fields(game.MovesUCI)
		for i, uci := range parts {
			opt, err := chess.FEN(pos.String())
			if err != nil {
				break
			}
			g := chess.NewGame(opt)
			n := chess.UCINotation{}
			mv, err := n.Decode(g.Position(), uci)
			if err != nil {
				break
			}
			san := chess.AlgebraicNotation{}.Encode(g.Position(), mv)
			if err := g.Move(mv); err != nil {
				break
			}
			pos = g.Position()
			moves = append(moves, GameMoveView{Index: i + 1, UCI: uci, SAN: san})
			positions = append(positions, GamePositionView{Index: i + 1, Board: boardFromPosition(pos)})
		}
	}

	return GameView{
		ID:          game.ID,
		PlayedAt:    game.PlayedAt,
		White:       game.White,
		Black:       game.Black,
		MovetimeMS:  game.MovetimeMS,
		Result:      game.Result,
		Termination: game.Termination,
		Moves:       moves,
		Positions:   positions,
	}, nil
}

func (h *Handler) handleMatchupMoves(w http.ResponseWriter, r *http.Request) {
	aIDStr := strings.TrimSpace(r.URL.Query().Get("a_id"))
	bIDStr := strings.TrimSpace(r.URL.Query().Get("b_id"))
	aName := strings.TrimSpace(r.URL.Query().Get("a"))
	bName := strings.TrimSpace(r.URL.Query().Get("b"))
	movetimeStr := strings.TrimSpace(r.URL.Query().Get("movetime"))
	if movetimeStr == "" {
		http.Error(w, "missing movetime", http.StatusBadRequest)
		return
	}
	movetime, err := strconv.Atoi(movetimeStr)
	if err != nil {
		http.Error(w, "invalid movetime", http.StatusBadRequest)
		return
	}

	aID := int64(0)
	if aIDStr != "" {
		if v, err := strconv.ParseInt(aIDStr, 10, 64); err == nil {
			aID = v
		}
	}
	bID := int64(0)
	if bIDStr != "" {
		if v, err := strconv.ParseInt(bIDStr, 10, 64); err == nil {
			bID = v
		}
	}

	if aID == 0 && aName != "" {
		id, err := h.store.EngineIDByName(r.Context(), aName)
		if err == nil {
			aID = id
		}
	}
	if bID == 0 && bName != "" {
		id, err := h.store.EngineIDByName(r.Context(), bName)
		if err == nil {
			bID = id
		}
	}
	if aID == 0 || bID == 0 {
		http.Error(w, "missing a_id/b_id", http.StatusBadRequest)
		return
	}
	lines, err := h.store.MatchupMovesLines(r.Context(), aID, bID, movetime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if aName == "" {
		if eng, err := h.store.EngineByID(r.Context(), aID); err == nil {
			aName = eng.Name
		}
	}
	if bName == "" {
		if eng, err := h.store.EngineByID(r.Context(), bID); err == nil {
			bName = eng.Name
		}
	}
	if aName == "" {
		aName = "engine"
	}
	if bName == "" {
		bName = "engine"
	}
	filename := fmt.Sprintf("matchup-%s-vs-%s-%dms.txt", aName, bName, movetime)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", sanitizeFilename(filename)))
	_, _ = w.Write([]byte(lines))
}

func (h *Handler) handleMatchupDelete(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	aIDStr := strings.TrimSpace(r.Form.Get("a_id"))
	bIDStr := strings.TrimSpace(r.Form.Get("b_id"))
	movetimeStr := strings.TrimSpace(r.Form.Get("movetime"))
	if movetimeStr == "" {
		http.Error(w, "missing movetime", http.StatusBadRequest)
		return
	}
	movetime, err := strconv.Atoi(movetimeStr)
	if err != nil {
		http.Error(w, "invalid movetime", http.StatusBadRequest)
		return
	}
	aID, err := strconv.ParseInt(aIDStr, 10, 64)
	if err != nil || aID == 0 {
		http.Error(w, "invalid a_id", http.StatusBadRequest)
		return
	}
	bID, err := strconv.ParseInt(bIDStr, 10, 64)
	if err != nil || bID == 0 {
		http.Error(w, "invalid b_id", http.StatusBadRequest)
		return
	}
	if _, err := h.store.DeleteMatchupGames(r.Context(), aID, bID, movetime); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/games", http.StatusSeeOther)
}

func sanitizeFilename(name string) string {
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, "\\", "-")
	return name
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
