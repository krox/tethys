package app

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"sync"

	"tethys/internal/config"
	"tethys/internal/configstore"
	"tethys/internal/db"
	"tethys/internal/engine"
	"tethys/internal/web"
)

type App struct {
	cfg config.Config
	db  *sql.DB

	runner *engine.Runner
	mux    *http.ServeMux

	closeOnce sync.Once
}

func New(cfg config.Config) (*App, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	sqlDB, err := db.Open(cfg.GamesDBPath)
	if err != nil {
		return nil, err
	}

	if err := db.Migrate(sqlDB); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}

	gameStore := db.NewStore(sqlDB)
	configStore, err := configstore.New(cfg.ConfigPath)
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	if err := autoConfigureEngines(configStore); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	b := engine.NewBroadcaster()
	r := engine.NewRunner(gameStore, configStore, b)
	r.Start(context.Background())

	h := web.NewHandler(cfg, gameStore, configStore, r, b)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	return &App{
		cfg:    cfg,
		db:     sqlDB,
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
		_ = a.db.Close()
	})
}

func autoConfigureEngines(store *configstore.Store) error {
	ctx := context.Background()
	cfg, err := store.GetConfig(ctx)
	if err != nil {
		return err
	}

	if len(cfg.Engines) > 0 {
		return nil
	}

	stockfishPath := firstExistingPath([]string{
		"/usr/games/stockfish",
		"/usr/bin/stockfish",
		"/bin/stockfish",
	})
	if stockfishPath == "" {
		return nil
	}

	cfg.Engines = []configstore.EngineConfig{
		{
			Name: "stockfish",
			Path: stockfishPath,
		},
	}
	return store.UpdateConfig(ctx, cfg)
}

func firstExistingPath(candidates []string) string {
	for _, p := range candidates {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}
