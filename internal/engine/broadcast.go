package engine

import (
	"net/http"
	"sync"
)

type Broadcaster struct {
	mu   sync.Mutex
	next int
	subs map[int]chan struct{}
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subs: make(map[int]chan struct{})}
}

func (b *Broadcaster) Subscribe() (id int, ch <-chan struct{}, unsubscribe func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id = b.next
	b.next++

	c := make(chan struct{}, 1)
	b.subs[id] = c

	return id, c, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if c2, ok := b.subs[id]; ok {
			delete(b.subs, id)
			close(c2)
		}
	}
}

func (b *Broadcaster) Publish() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func SSEHandler(b *Broadcaster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		_, ch, unsubscribe := b.Subscribe()
		defer unsubscribe()

		// initial ping
		_, _ = w.Write([]byte("event: update\ndata: 1\n\n"))
		flusher.Flush()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				_, _ = w.Write([]byte("event: update\ndata: 1\n\n"))
				flusher.Flush()
			}
		}
	}
}
