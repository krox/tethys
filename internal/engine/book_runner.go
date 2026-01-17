package engine

import (
	"os"

	"github.com/notnil/chess"

	"tethys/internal/book"
	"tethys/internal/configstore"
)

func (r *Runner) bookMove(pos *chess.Position, ply int, assignment configstore.ColorAssignment) (string, bool) {
	if !assignment.BookEnabled || assignment.BookPath == "" {
		return "", false
	}
	if assignment.BookMaxPlies > 0 && ply >= assignment.BookMaxPlies {
		return "", false
	}

	bookObj, err := r.loadBook(assignment.BookPath)
	if err != nil || bookObj == nil {
		return "", false
	}

	move, ok := bookObj.Lookup(pos)
	return move, ok
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
