package config

import (
	"os"
	"path/filepath"
)

type Config struct {
	ListenAddr  string
	DataDir     string
	GamesDBPath string
	ConfigPath  string
}

func FromEnv() Config {
	listenAddr := getenv("TETHYS_LISTEN_ADDR", ":8080")
	dataDir := getenv("TETHYS_DATA_DIR", "./data")
	gamesDBPath := getenv("TETHYS_GAMES_DB_PATH", filepath.Join(dataDir, "games.sqlite"))
	configPath := getenv("TETHYS_CONFIG_PATH", filepath.Join(dataDir, "config.json"))

	return Config{
		ListenAddr:  listenAddr,
		DataDir:     dataDir,
		GamesDBPath: gamesDBPath,
		ConfigPath:  configPath,
	}
}

func getenv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
