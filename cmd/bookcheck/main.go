package main

import (
	"fmt"
	"os"

	"github.com/notnil/chess"
	"tethys/internal/book"
)

func main() {
	path := "data/Performance.bin"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	b, err := book.Load(path)
	if err != nil {
		fmt.Println("load error:", err)
		return
	}
	pos := chess.StartingPosition()
	move, ok := b.Lookup(pos)
	fmt.Println("startpos move:", move, "ok:", ok)
}
