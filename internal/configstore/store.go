package configstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type EngineConfig struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Args string `json:"args"`
	Init string `json:"init"`
}

type PairConfig struct {
	AID int64  `json:"a_id,omitempty"`
	BID int64  `json:"b_id,omitempty"`
	A   string `json:"a,omitempty"`
	B   string `json:"b,omitempty"`
}

type Config struct {
	Engines      []EngineConfig `json:"engines"`
	EnabledPairs []PairConfig   `json:"enabled_pairs"`
	MovetimeMS   int            `json:"movetime_ms"`
	OpeningMin   int            `json:"opening_min_count"`
	BookEnabled  bool           `json:"book_enabled"`
	BookPath     string         `json:"book_path"`
	BookMaxPlies int            `json:"book_max_plies"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

type Store struct {
	path string
	mu   sync.Mutex
	cfg  Config
}

func New(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create config dir: %w", err)
	}

	store := &Store{path: path}
	baseDir := filepath.Dir(path)
	if err := store.loadOrInit(baseDir); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) GetConfig(ctx context.Context) (Config, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg, nil
}

func (s *Store) UpdateConfig(ctx context.Context, cfg Config) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range cfg.EnabledPairs {
		if cfg.EnabledPairs[i].AID != 0 || cfg.EnabledPairs[i].BID != 0 {
			cfg.EnabledPairs[i].A = ""
			cfg.EnabledPairs[i].B = ""
		}
	}

	cfg.UpdatedAt = time.Now().UTC()
	s.cfg = cfg
	return s.saveLocked()
}

func (s *Store) loadOrInit(baseDir string) error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.cfg = defaultConfig(baseDir)
			return s.saveLocked()
		}
		return fmt.Errorf("read config: %w", err)
	}

	if err := json.Unmarshal(data, &s.cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if s.cfg.MovetimeMS <= 0 {
		s.cfg.MovetimeMS = 100
	}
	if s.cfg.OpeningMin <= 0 {
		s.cfg.OpeningMin = 20
	}
	if s.cfg.BookMaxPlies <= 0 {
		s.cfg.BookMaxPlies = 16
	}
	if s.cfg.BookPath == "" {
		s.cfg.BookPath = filepath.Join(baseDir, "book.bin")
	}
	if len(s.cfg.Engines) == 0 {
		var legacy struct {
			EngineA EngineConfig `json:"engine_a"`
			EngineB EngineConfig `json:"engine_b"`
		}
		if err := json.Unmarshal(data, &legacy); err == nil {
			engines := make([]EngineConfig, 0, 2)
			if legacy.EngineA.Path != "" {
				legacy.EngineA.Name = "engine-a"
				engines = append(engines, legacy.EngineA)
			}
			if legacy.EngineB.Path != "" {
				legacy.EngineB.Name = "engine-b"
				engines = append(engines, legacy.EngineB)
			}
			if len(engines) > 0 {
				s.cfg.Engines = engines
				_ = s.saveLocked()
			}
		}
	}
	return nil
}

func (s *Store) saveLocked() error {
	data, err := json.MarshalIndent(s.cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(s.path, data, 0o644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func defaultConfig(baseDir string) Config {
	return Config{
		MovetimeMS:   100,
		OpeningMin:   20,
		BookEnabled:  false,
		BookPath:     filepath.Join(baseDir, "book.bin"),
		BookMaxPlies: 16,
		UpdatedAt:    time.Now().UTC(),
	}
}
