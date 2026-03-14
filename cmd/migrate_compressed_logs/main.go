package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"tethys/internal/db"
)

func main() {
	var (
		dataDir = flag.String("data-dir", "./data", "directory containing tethys.sqlite")
		dbPath  = flag.String("db", "", "path to sqlite database (overrides -data-dir)")
		batch   = flag.Int("batch", 250, "number of game ids to process per fetch")
	)
	flag.Parse()

	path := *dbPath
	if path == "" {
		path = filepath.Join(*dataDir, "tethys.sqlite")
	}
	if *batch <= 0 {
		*batch = 250
	}

	store, err := db.Open(path)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	var (
		afterID  int64
		migrated int
	)

	for {
		ids, err := store.ListGameIDsMissingCompressedLogs(ctx, afterID, *batch)
		if err != nil {
			log.Fatalf("list missing compressed logs: %v", err)
		}
		if len(ids) == 0 {
			break
		}
		for _, gameID := range ids {
			logs, err := store.ListEngineLogsByGame(ctx, gameID)
			if err != nil {
				log.Fatalf("load legacy logs for game %d: %v", gameID, err)
			}
			if err := store.InsertCompressedEngineLogs(ctx, gameID, logs); err != nil {
				log.Fatalf("insert compressed logs for game %d: %v", gameID, err)
			}
			migrated++
			afterID = gameID
		}
		log.Printf("migrated %d games so far (last game_id=%d)", migrated, afterID)
	}

	fmt.Printf("done: migrated %d games into compressed_engine_logs\n", migrated)
}
