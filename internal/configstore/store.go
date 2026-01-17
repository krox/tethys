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
	Path string `json:"path"`
	Args string `json:"args"`
	Init string `json:"init"`
}

type Config struct {
	EngineA      EngineConfig `json:"engine_a"`
	EngineB      EngineConfig `json:"engine_b"`
	MovetimeMS   int          `json:"movetime_ms"`
	Selfplay     bool         `json:"selfplay"`
	MaxPlies     int          `json:"max_plies"`
	OpeningMin   int          `json:"opening_min_count"`
	BookEnabled  bool         `json:"book_enabled"`
	BookPath     string       `json:"book_path"`
	BookMaxPlies int          `json:"book_max_plies"`
	NextAIsWhite bool         `json:"next_a_is_white"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

type ColorAssignment struct {
	White      EngineConfig
	Black      EngineConfig
	WhiteName  string
	BlackName  string
	MovetimeMS int
	Selfplay   bool
	MaxPlies   int
	BookEnabled  bool
	BookPath     string
	BookMaxPlies int
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

	cfg.NextAIsWhite = s.cfg.NextAIsWhite
	cfg.UpdatedAt = time.Now().UTC()
	s.cfg = cfg
	return s.saveLocked()
}

// GetAndToggleAssignment returns the color assignment for the next game and flips it for following games.
func (s *Store) GetAndToggleAssignment(ctx context.Context) (ColorAssignment, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	assign := ColorAssignment{
		MovetimeMS: s.cfg.MovetimeMS,
		Selfplay:   s.cfg.Selfplay,
		MaxPlies:   s.cfg.MaxPlies,
		BookEnabled:  s.cfg.BookEnabled,
		BookPath:     s.cfg.BookPath,
		BookMaxPlies: s.cfg.BookMaxPlies,
	}
	if assign.MovetimeMS <= 0 {
		assign.MovetimeMS = 100
	}
	if assign.MaxPlies <= 0 {
		assign.MaxPlies = 200
	}
	if assign.BookMaxPlies <= 0 {
		assign.BookMaxPlies = 16
	}

	if s.cfg.Selfplay {
		assign.White, assign.Black = s.cfg.EngineA, s.cfg.EngineA
		assign.WhiteName, assign.BlackName = "A", "A"
	} else if s.cfg.NextAIsWhite {
		assign.White, assign.Black = s.cfg.EngineA, s.cfg.EngineB
		assign.WhiteName, assign.BlackName = "A", "B"
	} else {
		assign.White, assign.Black = s.cfg.EngineB, s.cfg.EngineA
		assign.WhiteName, assign.BlackName = "B", "A"
	}

	s.cfg.NextAIsWhite = !s.cfg.NextAIsWhite
	s.cfg.UpdatedAt = time.Now().UTC()
	if err := s.saveLocked(); err != nil {
		return ColorAssignment{}, err
	}
	return assign, nil
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
	if s.cfg.MaxPlies <= 0 {
		s.cfg.MaxPlies = 200
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
		Selfplay:     false,
		MaxPlies:     200,
		OpeningMin:   20,
		BookEnabled:  false,
		BookPath:     filepath.Join(baseDir, "book.bin"),
		BookMaxPlies: 16,
		NextAIsWhite: true,
		UpdatedAt:    time.Now().UTC(),
	}
}
