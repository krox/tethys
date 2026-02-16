# tethys

A small self-play chess service: runs UCI engines on the server, plays games forever, stores games in SQLite, and exposes a public web UI to watch the current game and download past games.

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
   go run ./cmd/tethys
   ```

3. Open:

- Public UI: http://localhost:8080/
- Admin UI:  link with admin token will be printed to terminal on startup

## Configuration

Environment variables:

- `TETHYS_LISTEN_ADDR` (default `:8080`)
- `TETHYS_DATA_DIR` (default `./data`)

Storage locations (relative to `$TETHYS_DATA_DIR`):
- database: `tethys.sqlite`
- uploads: `engine_bins/`

Engine settings are stored in a JSON file and edited in the admin UI.

## Opening book (optional)

You can enable a Polyglot opening book in the admin UI. The default path is under the data folder as `book.bin`.