package web

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/notnil/chess"

	"tethys/internal/book"
	"tethys/internal/db"
	"tethys/internal/engine"
)

type PositionEvalResponse struct {
	ZobristKey uint64 `json:"zobrist_key"`
	Score      string `json:"score"`
	PV         string `json:"pv"`
	EngineID   int64  `json:"engine_id"`
	Depth      int    `json:"depth"`
	Done       bool   `json:"done"`
	Error      string `json:"error"`
}

func (h *Handler) handlePositionView(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	zobristStr := strings.TrimSpace(r.URL.Query().Get("zobrist"))
	fenParam := strings.TrimSpace(r.URL.Query().Get("fen"))

	var fenKey string
	var fullFen string
	var key uint64
	var err error
	if fenParam != "" {
		fenKey, fullFen, err = normalizeFENForView(fenParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		key, err = zobristFromFEN(fullFen)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else if zobristStr != "" {
		key, _ = strconv.ParseUint(zobristStr, 10, 64)
		if key == 0 {
			http.Error(w, "invalid zobrist", http.StatusBadRequest)
			return
		}
		if cached, err := h.store.EvalByZobrist(ctx, key); err == nil {
			fenKey = cached.FEN
			fullFen = cached.FEN + " 0 1"
		}
	} else {
		start := chess.StartingPosition()
		fullFen = start.String()
		fenKey, _, err = normalizeFENForView(fullFen)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		key, err = zobristFromFEN(fullFen)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	if fenKey == "" || fullFen == "" || key == 0 {
		http.Error(w, "missing fen or cached evaluation", http.StatusBadRequest)
		return
	}

	info, _ := h.an.EnsureAnalysis(ctx, fullFen)
	pos, err := positionFromFEN(fullFen)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	engineName := ""
	if info.EngineID != 0 {
		if e, err := h.store.EngineByID(ctx, info.EngineID); err == nil {
			engineName = e.Name
		}
	}
	_ = h.tpl.ExecuteTemplate(w, "position_view.html", map[string]any{
		"IsAdmin":    h.isAdminRequest(w, r),
		"Page":       "positions",
		"FEN":        fenKey,
		"ZobristKey": key,
		"Board":      boardFromPosition(pos),
		"Eval":       info,
		"EngineName": engineName,
	})
}

func (h *Handler) handlePositionEval(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	zobristStr := strings.TrimSpace(r.URL.Query().Get("zobrist"))
	if zobristStr == "" {
		http.Error(w, "missing zobrist", http.StatusBadRequest)
		return
	}
	key, _ := strconv.ParseUint(zobristStr, 10, 64)
	if key == 0 {
		http.Error(w, "invalid zobrist", http.StatusBadRequest)
		return
	}
	info, ok := h.an.Latest(key)
	if !ok {
		if cached, err := h.store.EvalByZobrist(ctx, key); err == nil {
			info = engineToAnalysisInfo(cached)
		} else if err != sql.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
		}
	}

	resp := PositionEvalResponse{
		ZobristKey: key,
		Score:      info.Score,
		PV:         info.PV,
		EngineID:   info.EngineID,
		Depth:      info.Depth,
		Done:       info.Done,
		Error:      info.Err,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

type PositionMoveResponse struct {
	FEN        string `json:"fen"`
	ZobristKey uint64 `json:"zobrist_key"`
}

func (h *Handler) handlePositionMove(w http.ResponseWriter, r *http.Request) {
	fenParam := strings.TrimSpace(r.URL.Query().Get("fen"))
	uci := strings.TrimSpace(r.URL.Query().Get("uci"))
	if fenParam == "" || uci == "" {
		http.Error(w, "missing fen or uci", http.StatusBadRequest)
		return
	}
	_, fullFen, err := normalizeFENForView(fenParam)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	opt, err := chess.FEN(fullFen)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	game := chess.NewGame(opt)
	n := chess.UCINotation{}
	mv, err := n.Decode(game.Position(), uci)
	if err != nil {
		http.Error(w, "invalid move", http.StatusBadRequest)
		return
	}
	if err := game.Move(mv); err != nil {
		http.Error(w, "illegal move", http.StatusBadRequest)
		return
	}
	pos := game.Position()
	fenKey, _, err := normalizeFENForView(pos.String())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	key, err := zobristFromFEN(pos.String())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := PositionMoveResponse{FEN: fenKey, ZobristKey: key}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func normalizeFENForView(fen string) (string, string, error) {
	parts := strings.Fields(strings.TrimSpace(fen))
	if len(parts) < 4 {
		return "", "", fmt.Errorf("invalid FEN")
	}
	fenKey := strings.Join(parts[:4], " ")
	full := fenKey + " 0 1"
	if _, err := chess.FEN(full); err != nil {
		return "", "", err
	}
	return fenKey, full, nil
}

func zobristFromFEN(fullFen string) (uint64, error) {
	opt, err := chess.FEN(fullFen)
	if err != nil {
		return 0, err
	}
	game := chess.NewGame(opt)
	pos := game.Position()
	return book.ZobristKey(pos), nil
}

func positionFromFEN(fullFen string) (*chess.Position, error) {
	opt, err := chess.FEN(fullFen)
	if err != nil {
		return nil, err
	}
	game := chess.NewGame(opt)
	return game.Position(), nil
}

func engineToAnalysisInfo(e db.Eval) engine.AnalysisInfo {
	return engine.AnalysisInfo{
		ZobristKey: e.ZobristKey,
		FEN:        e.FEN,
		Score:      e.Score,
		PV:         e.PV,
		EngineID:   e.EngineID,
		Depth:      e.Depth,
	}
}
