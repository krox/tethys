package engine

import (
	"os"

	"github.com/notnil/chess"

	"tethys/internal/book"
)

func (r *Runner) bookLine(start *chess.Position, assignment ColorAssignment) []string {
	if !assignment.BookEnabled || assignment.BookPath == "" {
		return nil
	}

	bookObj, err := r.loadBook(assignment.BookPath)
	if err != nil || bookObj == nil {
		return nil
	}

	maxPlies := assignment.BookMaxPlies
	line := make([]string, 0, 8)
	pos := start
	notation := chess.UCINotation{}

	for {
		if maxPlies > 0 && len(line) >= maxPlies {
			break
		}
		move, ok := bookObj.Lookup(pos)
		if !ok {
			break
		}
		mv, err := notation.Decode(pos, move)
		if err != nil {
			break
		}
		line = append(line, move)
		pos = pos.Update(mv)
	}

	return line
}

func (r *Runner) loadBook(path string) (*book.Book, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	r.bookMu.Lock()
	defer r.bookMu.Unlock()
	if r.book != nil && r.bookPath == path && r.bookMod.Equal(info.ModTime()) {
		return r.book, nil
	}

	b, err := book.Load(path)
	if err != nil {
		return nil, err
	}
	r.book = b
	r.bookPath = path
	r.bookMod = info.ModTime()
	return r.book, nil
}
