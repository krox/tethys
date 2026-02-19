package web

import (
	"net/http"
	"strings"

	"github.com/notnil/chess"

	"tethys/internal/book"
)

type BookMoveView struct {
	UCI     string
	SAN     string
	Weight  int
	Percent float64
	NextFEN string
}

type ArrowView struct {
	X1      float64
	Y1      float64
	X2      float64
	Y2      float64
	Opacity float64
}

func (h *Handler) handleBookExplorer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	view := map[string]any{
		"Page": "book",
	}

	settings, err := h.store.GetSettings(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bookPath := strings.TrimSpace(settings.GameBookPath)

	if bookPath == "" {
		view["Error"] = "No opening book configured."
		_ = h.tpl.ExecuteTemplate(w, "book_explorer.html", view)
		return
	}

	bk, err := book.Load(bookPath)
	if err != nil {
		view["Error"] = err.Error()
		_ = h.tpl.ExecuteTemplate(w, "book_explorer.html", view)
		return
	}

	fen := strings.TrimSpace(r.URL.Query().Get("fen"))
	pos := chess.StartingPosition()
	if fen != "" {
		opt, err := chess.FEN(fen)
		if err != nil {
			view["Error"] = "Invalid FEN."
			_ = h.tpl.ExecuteTemplate(w, "book_explorer.html", view)
			return
		}
		game := chess.NewGame(opt)
		pos = game.Position()
	} else {
		fen = pos.String()
	}

	moves := bk.Moves(pos)
	total := 0
	for _, mv := range moves {
		total += mv.Weight
	}

	moveViews := make([]BookMoveView, 0, len(moves))
	for _, mv := range moves {
		moveView := BookMoveView{UCI: mv.UCI, Weight: mv.Weight}
		if total > 0 {
			moveView.Percent = float64(mv.Weight) * 100 / float64(total)
		}
		opt, err := chess.FEN(pos.String())
		if err != nil {
			moveViews = append(moveViews, moveView)
			continue
		}
		game := chess.NewGame(opt)
		n := chess.UCINotation{}
		decoded, err := n.Decode(game.Position(), mv.UCI)
		if err == nil {
			moveView.SAN = chess.AlgebraicNotation{}.Encode(game.Position(), decoded)
			if err := game.Move(decoded); err == nil {
				moveView.NextFEN = game.Position().String()
			}
		}
		moveViews = append(moveViews, moveView)
	}

	view["BookPath"] = bookPath
	view["FEN"] = fen
	view["Moves"] = moveViews
	view["Board"] = boardFromPosition(pos)
	view["Arrows"] = arrowsFromMoves(moves, total)
	_ = h.tpl.ExecuteTemplate(w, "book_explorer.html", view)
}

func arrowsFromMoves(moves []book.MoveWeight, total int) []ArrowView {
	if len(moves) == 0 {
		return nil
	}
	out := make([]ArrowView, 0, len(moves))
	for _, mv := range moves {
		if len(mv.UCI) < 4 {
			continue
		}
		fromFile := mv.UCI[0]
		fromRank := mv.UCI[1]
		toFile := mv.UCI[2]
		toRank := mv.UCI[3]
		x1, y1, ok1 := squareCenter(fromFile, fromRank)
		x2, y2, ok2 := squareCenter(toFile, toRank)
		if !ok1 || !ok2 {
			continue
		}
		opacity := 0.35
		if total > 0 {
			pct := float64(mv.Weight) / float64(total)
			opacity = 0.25 + 0.65*pct
		}
		out = append(out, ArrowView{X1: x1, Y1: y1, X2: x2, Y2: y2, Opacity: opacity})
	}
	return out
}

func squareCenter(file, rank byte) (float64, float64, bool) {
	if file < 'a' || file > 'h' || rank < '1' || rank > '8' {
		return 0, 0, false
	}
	fileIdx := float64(file - 'a')
	rankIdx := float64(rank - '1')
	// viewBox is 0..8 with rank 8 at top.
	x := fileIdx + 0.5
	y := 8 - rankIdx - 0.5
	return x, y, true
}
