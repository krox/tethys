# tethys

A small self-play chess service: runs two UCI engines on the server, plays games forever, stores games in SQLite, and exposes a public web UI to watch the current game and download past games.

## Features

- Server-side UCI engine runner (A vs B), alternating colors each game
- Fixed `movetime` (ms) per move (configurable)
- Arbitrary per-engine init commands (sent to the engine before each game)
- Public UI: live game + past games + moves-only downloads
- Admin UI (single password): configure engines + restart runner

## Requirements

- Go (1.22+)
- SQLite (embedded via Go driver)

## Quickstart

1. Install Go (Debian/Ubuntu):

   ```bash
   sudo apt-get update
   sudo apt-get install -y golang
   ```

2. Run:

   ```bash
   cd tethys
   mkdir -p data
   TETHYS_ADMIN_PASSWORD=changeme go run ./cmd/tethys
   ```

3. Open:

- Public UI: http://localhost:8080/
- Admin UI:  http://localhost:8080/admin

## Configuration

Environment variables:

- `TETHYS_LISTEN_ADDR` (default `:8080`)
- `TETHYS_DATA_DIR` (default `./data`)
- `TETHYS_GAMES_DB_PATH` (default `$TETHYS_DATA_DIR/games.sqlite`)
- `TETHYS_CONFIG_PATH` (default `$TETHYS_DATA_DIR/config.json`)
- `TETHYS_ADMIN_PASSWORD` (required to use `/admin`)

Engine settings are stored in a JSON file and edited in the admin UI.

## Opening book (optional)

You can enable a Polyglot opening book in the admin UI. The default path is under the data folder as `book.bin`.
Note: the Polyglot *format* is standard, but each book file has its own license. Check the license of any book you download.
