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
	Name   string `json:"name"`
	Path   string `json:"path"`
	Args   string `json:"args"`
	Init   string `json:"init"`
	Active bool   `json:"active"`
}

type PairConfig struct {
	A string `json:"a"`
	B string `json:"b"`
}

type Config struct {
	Engines       []EngineConfig `json:"engines"`
	EnabledPairs  []PairConfig   `json:"enabled_pairs"`
	MovetimeMS    int            `json:"movetime_ms"`
	Selfplay      bool           `json:"selfplay"`
	MaxPlies      int            `json:"max_plies"`
	OpeningMin    int            `json:"opening_min_count"`
	BookEnabled   bool           `json:"book_enabled"`
	BookPath      string         `json:"book_path"`
	BookMaxPlies  int            `json:"book_max_plies"`
	NextPairIndex int            `json:"next_pair_index"`
	NextPairSwap  bool           `json:"next_pair_swap"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

type ColorAssignment struct {
	White        EngineConfig
	Black        EngineConfig
	WhiteName    string
	BlackName    string
	MovetimeMS   int
	Selfplay     bool
	MaxPlies     int
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

	cfg.NextPairIndex = s.cfg.NextPairIndex
	cfg.NextPairSwap = s.cfg.NextPairSwap
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
		MovetimeMS:   s.cfg.MovetimeMS,
		Selfplay:     s.cfg.Selfplay,
		MaxPlies:     s.cfg.MaxPlies,
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

	engineByName := make(map[string]EngineConfig)
	for _, e := range s.cfg.Engines {
		if e.Name == "" || e.Path == "" {
			continue
		}
		engineByName[e.Name] = e
	}

	validPairs := make([]PairConfig, 0, len(s.cfg.EnabledPairs))
	for _, p := range s.cfg.EnabledPairs {
		if p.A == "" || p.B == "" {
			continue
		}
		if _, ok := engineByName[p.A]; !ok {
			continue
		}
		if _, ok := engineByName[p.B]; !ok {
			continue
		}
		validPairs = append(validPairs, p)
	}

	if len(validPairs) > 0 {
		idx := s.cfg.NextPairIndex
		if idx < 0 || idx >= len(validPairs) {
			idx = 0
		}
		pair := validPairs[idx]
		white := engineByName[pair.A]
		black := engineByName[pair.B]
		if pair.A == pair.B {
			assign.White, assign.Black = white, white
			assign.WhiteName, assign.BlackName = pair.A, pair.A
			assign.Selfplay = true
			s.cfg.NextPairIndex = (idx + 1) % len(validPairs)
			s.cfg.NextPairSwap = false
		} else if s.cfg.NextPairSwap {
			assign.White, assign.Black = black, white
			assign.WhiteName, assign.BlackName = pair.B, pair.A
			assign.Selfplay = false
			s.cfg.NextPairIndex = (idx + 1) % len(validPairs)
			s.cfg.NextPairSwap = false
		} else {
			assign.White, assign.Black = white, black
			assign.WhiteName, assign.BlackName = pair.A, pair.B
			assign.Selfplay = false
			s.cfg.NextPairSwap = true
		}
	}

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
	if len(s.cfg.Engines) == 0 {
		var legacy struct {
			EngineA EngineConfig `json:"engine_a"`
			EngineB EngineConfig `json:"engine_b"`
		}
		if err := json.Unmarshal(data, &legacy); err == nil {
			engines := make([]EngineConfig, 0, 2)
			if legacy.EngineA.Path != "" {
				legacy.EngineA.Name = "engine-a"
				legacy.EngineA.Active = true
				engines = append(engines, legacy.EngineA)
			}
			if legacy.EngineB.Path != "" {
				legacy.EngineB.Name = "engine-b"
				legacy.EngineB.Active = true
				engines = append(engines, legacy.EngineB)
			}
			if len(engines) > 0 {
				s.cfg.Engines = engines
				_ = s.saveLocked()
			}
		}
	}
	if len(s.cfg.Engines) > 0 && len(s.cfg.EnabledPairs) == 0 {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err == nil {
			if _, ok := raw["enabled_pairs"]; !ok {
				pairs := make([]PairConfig, 0, len(s.cfg.Engines))
				for i := 0; i < len(s.cfg.Engines); i++ {
					for j := i; j < len(s.cfg.Engines); j++ {
						a := s.cfg.Engines[i].Name
						b := s.cfg.Engines[j].Name
						if a == "" || b == "" {
							continue
						}
						pairs = append(pairs, PairConfig{A: a, B: b})
					}
				}
				if len(pairs) > 0 {
					s.cfg.EnabledPairs = pairs
					_ = s.saveLocked()
				}
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
		MovetimeMS:    100,
		Selfplay:      false,
		MaxPlies:      200,
		OpeningMin:    20,
		BookEnabled:   false,
		BookPath:      filepath.Join(baseDir, "book.bin"),
		BookMaxPlies:  16,
		NextPairIndex: 0,
		NextPairSwap:  false,
		UpdatedAt:     time.Now().UTC(),
	}
}
