package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"tethys/internal/app"
)

func main() {
	listenAddr := getenv("TETHYS_LISTEN_ADDR", ":8080")
	dataDir := getenv("TETHYS_DATA_DIR", "./data")
	dbPath := filepath.Join(dataDir, "tethys.sqlite")
	configPath := filepath.Join(dataDir, "config.json")
	engineUploadDir := filepath.Join(dataDir, "engine_bins")

	application, err := app.New(dataDir, dbPath, configPath, engineUploadDir)
	if err != nil {
		log.Fatal(err)
	}
	defer application.Close()

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           application.Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("tethys listening on %s", listenAddr)
	log.Printf("admin token: %s", application.AdminToken())
	log.Printf("admin URL: %s", adminURL(listenAddr, application.AdminToken()))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func getenv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func adminURL(listenAddr, token string) string {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return fmt.Sprintf("http://%s/admin?token=%s", listenAddr, token)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s:%s/admin?token=%s", host, port, token)
}
