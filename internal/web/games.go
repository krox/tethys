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

	"tethys/internal/db"
)

type MatchupRow struct {
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

type OpeningRow struct {
	Opening string
	Wins    int
	Losses  int
	Draws   int
	Total   int
	WinPct  float64
	LossPct float64
	DrawPct float64
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
	openingRows, err := buildOpeningRows(ctx, h.store)
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
		"OpeningRows": openingRows,
		"IsAdmin":    h.isAdminRequest(w, r),
		"Page":       "games",
	})
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

func (h *Handler) handleOpeningDownload(w http.ResponseWriter, r *http.Request) {
	opening := strings.TrimSpace(r.URL.Query().Get("opening"))
	if opening == "" {
		http.Error(w, "missing opening", http.StatusBadRequest)
		return
	}
	lines, err := h.store.OpeningMovesLines(r.Context(), opening)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	filename := fmt.Sprintf("opening-%s.txt", sanitizeFilename(opening))
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	_, _ = w.Write([]byte(lines))
}

func (h *Handler) handleOpeningDelete(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	opening := strings.TrimSpace(r.Form.Get("opening"))
	if opening == "" {
		http.Error(w, "missing opening", http.StatusBadRequest)
		return
	}
	if _, err := h.store.DeleteOpeningGames(r.Context(), opening); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/games", http.StatusSeeOther)
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

func buildOpeningRows(ctx context.Context, store *db.Store) ([]OpeningRow, error) {
	rows, err := store.ListAllMovesWithResult(ctx)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]*OpeningRow)
	for _, row := range rows {
		if row.Result != "1-0" && row.Result != "0-1" && row.Result != "1/2-1/2" {
			continue
		}
		opening := openingKey(row.MovesUCI)
		entry, ok := counts[opening]
		if !ok {
			entry = &OpeningRow{Opening: opening}
			counts[opening] = entry
		}
		switch row.Result {
		case "1-0":
			entry.Wins++
		case "0-1":
			entry.Losses++
		case "1/2-1/2":
			entry.Draws++
		}
		entry.Total++
	}

	out := make([]OpeningRow, 0, len(counts))
	for _, entry := range counts {
		if entry.Total == 0 {
			continue
		}
		entry.WinPct = float64(entry.Wins) * 100 / float64(entry.Total)
		entry.LossPct = float64(entry.Losses) * 100 / float64(entry.Total)
		entry.DrawPct = float64(entry.Draws) * 100 / float64(entry.Total)
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Total == out[j].Total {
			return out[i].Opening < out[j].Opening
		}
		return out[i].Total > out[j].Total
	})
	return out, nil
}

func openingKey(movesUCI string) string {
	if strings.TrimSpace(movesUCI) == "" {
		return "(no moves)"
	}
	parts := strings.Fields(movesUCI)
	if len(parts) >= 2 {
		return parts[0] + " " + parts[1]
	}
	return parts[0]
}

func (h *Handler) handleMatchupMoves(w http.ResponseWriter, r *http.Request) {
	a := strings.TrimSpace(r.URL.Query().Get("a"))
	b := strings.TrimSpace(r.URL.Query().Get("b"))
	movetimeStr := strings.TrimSpace(r.URL.Query().Get("movetime"))
	if a == "" || b == "" || movetimeStr == "" {
		http.Error(w, "missing a/b/movetime", http.StatusBadRequest)
		return
	}
	movetime, err := strconv.Atoi(movetimeStr)
	if err != nil {
		http.Error(w, "invalid movetime", http.StatusBadRequest)
		return
	}
	lines, err := h.store.MatchupMovesLines(r.Context(), a, b, movetime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	filename := fmt.Sprintf("matchup-%s-vs-%s-%dms.txt", a, b, movetime)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", sanitizeFilename(filename)))
	_, _ = w.Write([]byte(lines))
}

func (h *Handler) handleMatchupDelete(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a := strings.TrimSpace(r.Form.Get("a"))
	b := strings.TrimSpace(r.Form.Get("b"))
	movetimeStr := strings.TrimSpace(r.Form.Get("movetime"))
	if a == "" || b == "" || movetimeStr == "" {
		http.Error(w, "missing a/b/movetime", http.StatusBadRequest)
		return
	}
	movetime, err := strconv.Atoi(movetimeStr)
	if err != nil {
		http.Error(w, "invalid movetime", http.StatusBadRequest)
		return
	}
	if _, err := h.store.DeleteMatchupGames(r.Context(), a, b, movetime); err != nil {
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
