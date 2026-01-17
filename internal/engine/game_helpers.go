package engine

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/notnil/chess"
)

func applyInit(ctx context.Context, e *UCIEngine, init string) error {
	lines := strings.Split(init, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if err := e.Send(line); err != nil {
			return err
		}
	}
	return e.IsReady(ctx)
}

func outcomeToResult(g *chess.Game) (result, termination string) {
	out := g.Outcome()
	method := g.Method()

	switch out {
	case chess.WhiteWon:
		result = "1-0"
	case chess.BlackWon:
		result = "0-1"
	case chess.Draw:
		result = "1/2-1/2"
	default:
		result = "*"
	}
	termination = method.String()
	return result, termination
}

func engineDisplayName(path string, fallback string) string {
	base := filepath.Base(path)
	if base == "." || base == "/" || base == "" {
		return fallback
	}
	return base
}

func boardFromPosition(pos *chess.Position) [][]SquareView {
	board := make([][]SquareView, 0, 8)
	b := pos.Board()

	for r := chess.Rank8; r >= chess.Rank1; r-- {
		row := make([]SquareView, 0, 8)
		for f := chess.FileA; f <= chess.FileH; f++ {
			sq := chess.NewSquare(f, r)
			p := b.Piece(sq)
			glyph := pieceGlyph(p)

			// a1 is dark.
			fileIdx := int(f)
			rankIdx := int(r)
			light := (fileIdx+rankIdx)%2 == 1
			class := "sq "
			if light {
				class += "light"
			} else {
				class += "dark"
			}

			row = append(row, SquareView{Glyph: glyph, Class: class})
		}
		board = append(board, row)
	}
	return board
}

func pieceGlyph(p chess.Piece) string {
	if p == chess.NoPiece {
		return ""
	}

	isWhite := p.Color() == chess.White
	switch p.Type() {
	case chess.King:
		if isWhite {
			return "♔"
		}
		return "♚"
	case chess.Queen:
		if isWhite {
			return "♕"
		}
		return "♛"
	case chess.Rook:
		if isWhite {
			return "♖"
		}
		return "♜"
	case chess.Bishop:
		if isWhite {
			return "♗"
		}
		return "♝"
	case chess.Knight:
		if isWhite {
			return "♘"
		}
		return "♞"
	case chess.Pawn:
		if isWhite {
			return "♙"
		}
		return "♟"
	default:
		return ""
	}
}
