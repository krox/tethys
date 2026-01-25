package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"

	"tethys/internal/configstore"
	"tethys/internal/db"
	"tethys/internal/engine"
	"tethys/internal/web"
)

type App struct {
	store *db.Store

	runner *engine.Runner
	mux    *http.ServeMux

	adminToken string

	closeOnce sync.Once
}

func New(dataDir string, dbPath string, configPath string, engineUploadDir string) (*App, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	if err := os.MkdirAll(engineUploadDir, 0o755); err != nil {
		return nil, fmt.Errorf("create engine upload dir: %w", err)
	}

	adminToken, _, err := loadOrInitAdminToken(dataDir)
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.Open(dbPath)
	if err != nil {
		return nil, err
	}
	configStore, err := configstore.New(configPath)
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	b := engine.NewBroadcaster()
	r := engine.NewRunner(sqlDB, configStore, b)
	r.Start(context.Background())

	h := web.NewHandler(sqlDB, configStore, r, b, adminToken, engineUploadDir)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	return &App{
		//db:         sqlDB,
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
		_ = a.store.Close()
	})
}
