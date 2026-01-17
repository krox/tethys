package web

import (
	"context"
	"sort"
	"strings"

	"github.com/notnil/chess"

	"tethys/internal/db"
)

type OpeningNode struct {
	Move       string
	Count      int
	WhiteWins  int
	BlackWins  int
	Draws      int
	Children   []*OpeningNode
	childrenBy map[string]*OpeningNode
}

type OpeningTree struct {
	MaxPlies int
	MinCount int
	Games    int
	Root     *OpeningNode
}

type gameMoves struct {
	MovesUCI string
	Result   string
}

func buildOpeningTree(ctx context.Context, store *db.Store, maxPlies, maxGames, minCount int) (OpeningTree, error) {
	games, err := store.ListFinishedGamesMoves(ctx, maxGames)
	if err != nil {
		return OpeningTree{}, err
	}

	root := &OpeningNode{}
	for _, g := range games {
		moves := strings.Fields(g.MovesUCI)
		if len(moves) == 0 {
			continue
		}
		limit := len(moves)
		if maxPlies > 0 && limit > maxPlies {
			limit = maxPlies
		}

		pos := chess.StartingPosition()
		notation := chess.UCINotation{}

		node := root
		for i := 0; i < limit; i++ {
			mv, err := notation.Decode(pos, moves[i])
			if err != nil {
				break
			}
			pos = pos.Update(mv)

			node = node.child(moves[i])
			node.Count++
			switch g.Result {
			case "1-0":
				node.WhiteWins++
			case "0-1":
				node.BlackWins++
			case "1/2-1/2":
				node.Draws++
			}
		}
	}

	root.finalize()
	root.prune(minCount, true)
	return OpeningTree{MaxPlies: maxPlies, MinCount: minCount, Games: len(games), Root: root}, nil
}

func (n *OpeningNode) child(move string) *OpeningNode {
	if n.childrenBy == nil {
		n.childrenBy = make(map[string]*OpeningNode)
	}
	if existing, ok := n.childrenBy[move]; ok {
		return existing
	}
	child := &OpeningNode{Move: move}
	n.childrenBy[move] = child
	n.Children = append(n.Children, child)
	return child
}

func (n *OpeningNode) finalize() {
	if len(n.Children) == 0 {
		return
	}
	sort.SliceStable(n.Children, func(i, j int) bool {
		return n.Children[i].Count > n.Children[j].Count
	})
	for _, c := range n.Children {
		c.finalize()
	}
}

func (n *OpeningNode) prune(minCount int, isRoot bool) {
	if !isRoot && minCount > 0 && n.Count < minCount {
		n.Children = nil
		return
	}
	if len(n.Children) == 0 {
		return
	}
	for _, c := range n.Children {
		c.prune(minCount, false)
	}
}
