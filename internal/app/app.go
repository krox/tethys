package app

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
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

	adminToken string

	closeOnce sync.Once
}

func New(cfg config.Config) (*App, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	if err := os.MkdirAll(cfg.EngineUploadDir, 0o755); err != nil {
		return nil, fmt.Errorf("create engine upload dir: %w", err)
	}

	adminToken, _, err := loadOrInitAdminToken(cfg.DataDir)
	if err != nil {
		return nil, err
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
	if err := ensureEnginesInDB(gameStore, configStore); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	b := engine.NewBroadcaster()
	r := engine.NewRunner(gameStore, configStore, b)
	r.Start(context.Background())

	h := web.NewHandler(gameStore, configStore, r, b, adminToken, cfg.EngineUploadDir)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	return &App{
		cfg:        cfg,
		db:         sqlDB,
		runner:     r,
		mux:        mux,
		adminToken: adminToken,
	}, nil
}

func (a *App) Router() http.Handler {
	return a.mux
}

func (a *App) AdminToken() string {
	return a.adminToken
}

func (a *App) Close() {
	a.closeOnce.Do(func() {
		a.runner.Stop()
		_ = a.db.Close()
	})
}

func ensureEnginesInDB(store *db.Store, conf *configstore.Store) error {
	ctx := context.Background()
	engines, err := store.ListEngines(ctx)
	if err != nil {
		return err
	}
	if len(engines) > 0 {
		return nil
	}
	if conf != nil {
		cfg, err := conf.GetConfig(ctx)
		if err != nil {
			return err
		}
		for _, e := range cfg.Engines {
			if e.Path == "" {
				continue
			}
			name := e.Name
			if name == "" {
				name = engineDisplayName(e.Path, "engine")
			}
			_, err := store.InsertEngine(ctx, db.Engine{
				Name: name,
				Path: e.Path,
				Args: e.Args,
				Init: e.Init,
			})
			if err != nil {
				return err
			}
		}
	}

	engines, err = store.ListEngines(ctx)
	if err != nil {
		return err
	}
	if len(engines) > 0 {
		return nil
	}

	return nil
}

func engineDisplayName(path string, fallback string) string {
	base := filepath.Base(path)
	if base == "." || base == "/" || base == "" {
		return fallback
	}
	return base
}
