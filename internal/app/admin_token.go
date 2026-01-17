package app

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func loadOrInitAdminToken(dataDir string) (string, bool, error) {
	path := filepath.Join(dataDir, "admin.token")
	data, err := os.ReadFile(path)
	if err == nil {
		token := strings.TrimSpace(string(data))
		if token != "" {
			return token, false, nil
		}
	} else if !os.IsNotExist(err) {
		return "", false, fmt.Errorf("read admin token: %w", err)
	}

	token, err := generateAdminToken()
	if err != nil {
		return "", false, err
	}
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		return "", false, fmt.Errorf("write admin token: %w", err)
	}
	return token, true, nil
}

func generateAdminToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate admin token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
