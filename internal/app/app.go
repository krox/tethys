package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"tethys/internal/db"
	"tethys/internal/engine"
	"tethys/internal/web"
)

type App struct {
	store *db.Store

	runner *engine.Runner
	mux    *http.ServeMux

	closeOnce sync.Once
}

func New(dataDir string, dbPath string) (*App, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	enginesDir := filepath.Join(dataDir, "engines")
	if err := os.MkdirAll(enginesDir, 0o755); err != nil {
		return nil, fmt.Errorf("create engines dir: %w", err)
	}
	booksDir := filepath.Join(dataDir, "books")
	if err := os.MkdirAll(booksDir, 0o755); err != nil {
		return nil, fmt.Errorf("create books dir: %w", err)
	}

	sqlDB, err := db.Open(dbPath)
	if err != nil {
		return nil, err
	}
	b := engine.NewBroadcaster()
	r := engine.NewRunner(sqlDB, b)
	r.Start(context.Background())
	an := engine.NewAnalyzer(sqlDB)

	h := web.NewHandler(sqlDB, r, b, an, enginesDir, booksDir)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	return &App{
		runner: r,
		mux:    mux,
	}, nil
}

func (a *App) Router() http.Handler {
	return a.mux
}

func (a *App) Close() {
	a.closeOnce.Do(func() {
		a.runner.Stop()
		_ = a.store.Close()
	})
}
