package web

import (
	"fmt"
	"strings"

	"github.com/notnil/chess"
)

type SquareView struct {
	Glyph  string
	Class  string
	Square string
	Piece  string
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
			piece := pieceCode(p)
			square := fmt.Sprintf("%c%d", 'a'+byte(f), int(r)+1)

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

			row = append(row, SquareView{Glyph: glyph, Class: class, Square: square, Piece: piece})
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

func pieceCode(p chess.Piece) string {
	if p == chess.NoPiece {
		return ""
	}
	letter := ""
	switch p.Type() {
	case chess.King:
		letter = "k"
	case chess.Queen:
		letter = "q"
	case chess.Rook:
		letter = "r"
	case chess.Bishop:
		letter = "b"
	case chess.Knight:
		letter = "n"
	case chess.Pawn:
		letter = "p"
	default:
		return ""
	}
	if p.Color() == chess.White {
		return strings.ToUpper(letter)
	}
	return letter
}
