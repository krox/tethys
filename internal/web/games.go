package web

import (
	"database/sql"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
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

func (h *Handler) handleGames(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	matchups, err := h.store.ListMatchupSummaries(ctx)
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
	_ = h.tpl.ExecuteTemplate(w, "games.html", map[string]any{
		"Rows":    rows,
		"IsAdmin": h.isAdminRequest(w, r),
		"Page":    "games",
	})
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
