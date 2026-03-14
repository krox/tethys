package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/klauspost/compress/zstd"
)

type compressedPlyLog struct {
	Log       string `json:"log"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

func (s *Store) InsertCompressedEngineLogs(ctx context.Context, gameID int64, logs []EngineLog) error {
	if len(logs) == 0 {
		return nil
	}

	jsonPayload, compressedPayload, err := encodeCompressedEngineLogs(logs)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO compressed_engine_logs (game_id, data, uncompressed_size)
		VALUES (?, ?, ?)
	`, gameID, compressedPayload, len(jsonPayload))
	return err
}

func (s *Store) CompressedEngineLogsByGame(ctx context.Context, gameID int64) (map[int]EngineLog, bool, error) {
	var row struct {
		Data []byte `db:"data"`
	}
	if err := s.db.GetContext(ctx, &row, `
		SELECT data
		FROM compressed_engine_logs
		WHERE game_id = ?
	`, gameID); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}

	logs, err := decodeCompressedEngineLogs(gameID, row.Data)
	if err != nil {
		return nil, true, err
	}
	return logs, true, nil
}

func encodeCompressedEngineLogs(logs []EngineLog) ([]byte, []byte, error) {
	payload := make(map[string]compressedPlyLog, len(logs))
	plyOrder := make([]int, 0, len(logs))
	seen := make(map[int]bool, len(logs))
	for _, entry := range logs {
		if entry.Ply <= 0 {
			continue
		}
		payload[strconv.Itoa(entry.Ply)] = compressedPlyLog{
			Log:       entry.Log,
			ElapsedMS: entry.ElapsedMS,
		}
		if !seen[entry.Ply] {
			seen[entry.Ply] = true
			plyOrder = append(plyOrder, entry.Ply)
		}
	}
	sort.Ints(plyOrder)

	ordered := make(map[string]compressedPlyLog, len(payload))
	for _, ply := range plyOrder {
		key := strconv.Itoa(ply)
		ordered[key] = payload[key]
	}

	jsonPayload, err := json.Marshal(ordered)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal compressed logs json: %w", err)
	}

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("create zstd encoder: %w", err)
	}
	defer encoder.Close()

	compressedPayload := encoder.EncodeAll(jsonPayload, make([]byte, 0, len(jsonPayload)))
	return jsonPayload, compressedPayload, nil
}

func decodeCompressedEngineLogs(gameID int64, compressed []byte) (map[int]EngineLog, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	defer decoder.Close()

	jsonPayload, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decode logs for game %d: %w", gameID, err)
	}

	var payload map[string]compressedPlyLog
	if err := json.Unmarshal(jsonPayload, &payload); err != nil {
		return nil, fmt.Errorf("json decode logs for game %d: %w", gameID, err)
	}

	out := make(map[int]EngineLog, len(payload))
	for plyKey, data := range payload {
		ply, err := strconv.Atoi(plyKey)
		if err != nil {
			return nil, fmt.Errorf("invalid ply key %q in compressed logs for game %d", plyKey, gameID)
		}
		out[ply] = EngineLog{
			GameID:    gameID,
			Ply:       ply,
			ElapsedMS: data.ElapsedMS,
			Log:       data.Log,
		}
	}
	return out, nil
}
